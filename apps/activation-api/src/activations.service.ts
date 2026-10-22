import { BadRequestException, Injectable, Logger, NotFoundException, OnModuleInit } from '@nestjs/common';
import {
  ActivationCompleted,
  ActivationFailed,
  BillingEvent,
  createEvent,
  FailedStep,
  ProvisioningEvent,
  SimulateCrash,
  SimulateFailure,
} from '@poc/contracts';
import { MongoService, OutboxService, TxContext } from '@poc/kafka-toolkit';
import { randomUUID } from 'crypto';
import { ClientSession, Collection } from 'mongodb';
import {
  ActivationDoc,
  ActivationStatus,
  CreateActivationDto,
  HistoryEntry,
  OPEN_STATUSES,
  StepResult,
} from './activation.model';

const SOURCE = 'activation-api';
const FAILURES: SimulateFailure[] = ['none', 'billing', 'provisioning'];
const CRASHES: SimulateCrash[] = ['none', 'billing', 'provisioning'];

type StepEvent = BillingEvent | ProvisioningEvent;

@Injectable()
export class ActivationsService implements OnModuleInit {
  private readonly logger = new Logger('Saga');

  constructor(
    private readonly mongo: MongoService,
    private readonly outbox: OutboxService,
  ) {}

  get col(): Collection<ActivationDoc> {
    return this.mongo.db.collection<ActivationDoc>('activations');
  }

  async onModuleInit(): Promise<void> {
    await this.col.createIndex({ customerId: 1 }, { name: 'customerId' });
    await this.col.createIndex({ status: 1, createdAt: 1 }, { name: 'status_createdAt' });
  }

  // ------------------------------------------------------------------ REST

  /** RF-02 / RF-03: guarda la activación y su ActivationRequested en una transacción (outbox). */
  async create(dto: CreateActivationDto): Promise<{ activationId: string; status: ActivationStatus; doc: ActivationDoc }> {
    const customerId = String(dto?.customerId ?? '').trim();
    const planId = String(dto?.planId ?? '').trim();
    const simulateFailure = (dto?.simulateFailure ?? 'none') as SimulateFailure;
    const simulateCrash = (dto?.simulateCrash ?? 'none') as SimulateCrash;

    if (!customerId || customerId.length > 40) throw new BadRequestException('customerId es obligatorio (máx. 40)');
    if (!planId || planId.length > 60) throw new BadRequestException('planId es obligatorio (máx. 60)');
    if (!FAILURES.includes(simulateFailure)) throw new BadRequestException(`simulateFailure inválido: ${simulateFailure}`);
    if (!CRASHES.includes(simulateCrash)) throw new BadRequestException(`simulateCrash inválido: ${simulateCrash}`);

    const activationId = `act-${randomUUID().slice(0, 8)}`;
    const event = createEvent('ActivationRequested', {
      correlationId: activationId,
      customerId,
      source: SOURCE,
      payload: {
        planId,
        channel: 'web',
        simulateFailure,
        ...(simulateCrash !== 'none' ? { simulateCrash } : {}),
      },
    });

    const now = new Date();
    const doc: ActivationDoc = {
      _id: activationId,
      customerId,
      planId,
      status: 'PENDING',
      simulateFailure,
      ...(simulateCrash !== 'none' ? { simulateCrash } : {}),
      steps: {},
      history: [{ eventType: event.eventType, at: event.occurredAt, source: SOURCE, eventId: event.eventId }],
      createdAt: now,
      updatedAt: now,
    };

    await this.mongo.withTransaction(async (session) => {
      await this.col.insertOne(doc, { session });
      await this.outbox.add(event, session);
    });
    this.outbox.kick();
    this.logger.log(`Nueva activación ${activationId} (${customerId}, ${planId}, fallo=${simulateFailure})`);
    return { activationId, status: 'PENDING', doc };
  }

  async get(id: string): Promise<ActivationDoc> {
    const doc = await this.col.findOne({ _id: id });
    if (!doc) throw new NotFoundException(`No existe la activación ${id}`);
    return doc;
  }

  findById(id: string): Promise<ActivationDoc | null> {
    return this.col.findOne({ _id: id });
  }

  list(limit = 20): Promise<ActivationDoc[]> {
    return this.col
      .find({})
      .sort({ createdAt: -1 })
      .limit(Math.min(Math.max(limit, 1), 100))
      .toArray();
  }

  /**
   * Demo de idempotencia: vuelve a publicar los eventos de la activación con EL MISMO eventId.
   * billing/provisioning/notification los descartan: no hay cuentas ni emails duplicados.
   */
  async resend(id: string): Promise<{ resent: number; doc: ActivationDoc | null }> {
    await this.get(id);
    const resent = await this.outbox.resend(id, ['ActivationRequested', 'ActivationCompleted', 'ActivationFailed']);
    const entry: HistoryEntry = {
      eventType: 'ManualResend',
      at: new Date().toISOString(),
      source: 'demo-ui',
      detail: `${resent} evento(s) reenviados con el mismo eventId`,
    };
    await this.col.updateOne({ _id: id }, { $push: { history: entry }, $set: { updatedAt: new Date() } });
    return { resent, doc: await this.findById(id) };
  }

  // ------------------------------------------------------------------ saga (agregador)

  /**
   * Regla de agregación (sección 06): guarda el resultado de cada servicio; con los dos OK
   * publica ActivationCompleted; con el primer fallo publica ActivationFailed. Los resultados
   * posteriores a un estado final solo se registran en el historial.
   */
  async onStepResult(e: StepEvent, tx: TxContext): Promise<void> {
    const now = new Date();
    const entry: HistoryEntry = {
      eventType: e.eventType,
      at: e.occurredAt,
      source: e.source,
      eventId: e.eventId,
      detail: 'reason' in e.payload ? e.payload.reason : undefined,
    };

    // La compensación no cambia el estado: solo queda en el historial
    if (e.eventType === 'BillingAccountCancelled') {
      await this.col.updateOne(
        { _id: e.correlationId },
        { $push: { history: entry }, $set: { updatedAt: now } },
        { session: tx.session },
      );
      this.logger.log(`${e.correlationId}: compensación confirmada (cuenta ${e.payload.accountId} anulada)`);
      return;
    }

    const step: 'billing' | 'provisioning' = e.eventType.startsWith('Billing') ? 'billing' : 'provisioning';
    const ok = e.eventType === 'BillingAccountCreated' || e.eventType === 'ProvisioningCompleted';
    const result: StepResult = { status: ok ? 'OK' : 'FAILED', at: e.occurredAt, eventId: e.eventId, data: e.payload as never };

    // 1) Registrar el resultado parcial
    const doc = await this.col.findOneAndUpdate(
      { _id: e.correlationId },
      { $set: { [`steps.${step}`]: result, updatedAt: now }, $push: { history: entry } },
      { session: tx.session, returnDocument: 'after' },
    );
    if (!doc) {
      this.logger.warn(`${e.eventType} para una activación desconocida (${e.correlationId}): se ignora`);
      return;
    }
    if (!OPEN_STATUSES.includes(doc.status)) {
      this.logger.log(`${doc._id}: ${e.eventType} llegó con la activación ya en ${doc.status} → solo historial`);
      return;
    }

    // 2) Decidir el próximo estado
    const { billing, provisioning } = doc.steps;
    const failedStep: FailedStep | null =
      billing?.status === 'FAILED' ? 'billing' : provisioning?.status === 'FAILED' ? 'provisioning' : null;
    const next: ActivationStatus = failedStep
      ? 'FAILED'
      : billing?.status === 'OK' && provisioning?.status === 'OK'
        ? 'ACTIVE'
        : 'IN_PROGRESS';

    if (next === 'IN_PROGRESS') {
      await this.transition(doc, next, null, tx.session);
      return;
    }

    const outcome =
      next === 'ACTIVE'
        ? this.completedEvent(doc)
        : this.failedEvent(doc, failedStep!, String((doc.steps[failedStep as 'billing' | 'provisioning']?.data as { reason?: string })?.reason ?? 'Error'));

    // 3) Solo publica quien logra el cambio de estado (update condicional)
    const won = await this.transition(doc, next, outcome, tx.session);
    if (won) {
      await tx.publish(outcome);
      this.logger.log(`${doc._id}: ${next} → publica ${outcome.eventType}`);
    }
  }

  /** Activaciones que no terminaron a tiempo (p. ej. el mensaje de billing fue a la DLQ). */
  async expire(doc: ActivationDoc, timeoutMs: number): Promise<boolean> {
    const missing = (['billing', 'provisioning'] as const).filter((s) => !doc.steps[s]);
    const reason = `Timeout: sin respuesta de ${missing.join(' y ') || 'los servicios'} en ${Math.round(timeoutMs / 1000)} s`;
    const outcome = this.failedEvent(doc, 'timeout', reason);
    const won = await this.mongo.withTransaction(async (session) => {
      const ok = await this.transition(doc, 'FAILED', outcome, session);
      if (ok) await this.outbox.add(outcome, session);
      return ok;
    });
    if (won) {
      this.outbox.kick();
      this.logger.warn(`${doc._id}: ${reason} → FAILED`);
    }
    return won;
  }

  private async transition(
    doc: ActivationDoc,
    next: ActivationStatus,
    outcome: ActivationCompleted | ActivationFailed | null,
    session: ClientSession,
  ): Promise<boolean> {
    const set: Partial<ActivationDoc> = { status: next, updatedAt: new Date() };
    if (outcome?.eventType === 'ActivationFailed') {
      set.failure = { step: outcome.payload.failedStep, reason: outcome.payload.reason };
    }
    const res = await this.col.updateOne(
      { _id: doc._id, status: { $in: OPEN_STATUSES } },
      {
        $set: set,
        ...(outcome
          ? {
              $push: {
                history: {
                  eventType: outcome.eventType,
                  at: outcome.occurredAt,
                  source: SOURCE,
                  eventId: outcome.eventId,
                  detail: outcome.eventType === 'ActivationFailed' ? outcome.payload.reason : undefined,
                },
              },
            }
          : {}),
      },
      { session },
    );
    return res.modifiedCount === 1;
  }

  private completedEvent(doc: ActivationDoc): ActivationCompleted {
    return createEvent('ActivationCompleted', {
      correlationId: doc._id,
      customerId: doc.customerId,
      source: SOURCE,
      payload: {
        planId: doc.planId,
        accountId: doc.steps.billing?.data?.accountId as string | undefined,
        lineNumber: doc.steps.provisioning?.data?.lineNumber as string | undefined,
      },
    });
  }

  private failedEvent(doc: ActivationDoc, failedStep: FailedStep, reason: string): ActivationFailed {
    return createEvent('ActivationFailed', {
      correlationId: doc._id,
      customerId: doc.customerId,
      source: SOURCE,
      payload: { planId: doc.planId, failedStep, reason },
    });
  }
}
