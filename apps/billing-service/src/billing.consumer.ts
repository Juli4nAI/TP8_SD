import { Controller, Logger, OnModuleInit } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { ActivationFailed, ActivationOutcomeEvent, ActivationRequested, createEvent, TOPICS } from '@poc/contracts';
import { MongoService, randomBetween, ReliableConsumer, sleep, TxContext } from '@poc/kafka-toolkit';
import { randomUUID } from 'crypto';
import { Collection } from 'mongodb';

const SOURCE = 'billing';

/**
 * CREATED           cuenta creada
 * CANCELLED         cuenta anulada por compensación
 * FAILED            no se pudo crear (fallo de negocio)
 * CANCEL_REQUESTED  llegó ActivationFailed ANTES de crear la cuenta (caso borde de la sección 06)
 */
type AccountStatus = 'CREATED' | 'CANCELLED' | 'FAILED' | 'CANCEL_REQUESTED';

interface BillingAccount {
  activationId: string;
  customerId: string;
  planId?: string;
  accountId?: string;
  status: AccountStatus;
  createdAt: Date;
  updatedAt: Date;
  cancelReason?: string;
}

/** Consumer group "billing-svc": activation.requested + activation.events */
@Controller()
export class BillingConsumer implements OnModuleInit {
  private readonly logger = new Logger('Billing');

  constructor(
    private readonly consumer: ReliableConsumer,
    private readonly mongo: MongoService,
  ) {}

  private get accounts(): Collection<BillingAccount> {
    return this.mongo.db.collection<BillingAccount>('billing_accounts');
  }

  async onModuleInit(): Promise<void> {
    await this.accounts.createIndex({ activationId: 1 }, { unique: true, name: 'activationId_unique' });
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Ctx() ctx: KafkaContext) {
    return this.consumer.process<ActivationRequested>(ctx, (e, tx) => this.createAccount(e, tx));
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationOutcome(@Ctx() ctx: KafkaContext) {
    return this.consumer.process<ActivationOutcomeEvent>(ctx, async (e, tx) => {
      if (e.eventType === 'ActivationFailed') await this.compensate(e, tx);
      // ActivationCompleted: no hay nada que hacer (queda registrado como procesado)
    });
  }

  /** ActivationRequested → crea la cuenta de facturación simulada. */
  private async createAccount(e: ActivationRequested, tx: TxContext): Promise<void> {
    const id = e.correlationId;

    // Error TÉCNICO simulado: excepción en cada intento → reintentos → DLQ
    if (e.payload.simulateCrash === 'billing') {
      throw new Error('Simulado: la base de facturación no responde');
    }

    await sleep(randomBetween(300, 900)); // latencia del sistema de facturación

    // Fallo de NEGOCIO simulado: es un resultado válido, no va a la DLQ
    if (e.payload.simulateFailure === 'billing') {
      await this.accounts.updateOne(
        { activationId: id },
        {
          $setOnInsert: { activationId: id, customerId: e.customerId, planId: e.payload.planId, createdAt: new Date() },
          $set: { status: 'FAILED', updatedAt: new Date() },
        },
        { upsert: true, session: tx.session },
      );
      await tx.publish(
        createEvent('BillingFailed', {
          correlationId: id,
          customerId: e.customerId,
          source: SOURCE,
          payload: { reason: 'Simulado: el cliente tiene deuda pendiente' },
        }),
      );
      this.logger.warn(`${id}: BillingFailed (simulado)`);
      return;
    }

    const existing = await this.accounts.findOne({ activationId: id }, { session: tx.session });
    if (existing && existing.status !== 'CANCEL_REQUESTED') {
      // Ya existe una cuenta para esta activación (índice único): no se crea otra
      this.logger.log(`${id}: la cuenta ya existía (${existing.status}), no se duplica`);
      return;
    }

    const accountId = `ACC-${randomUUID().slice(0, 8).toUpperCase()}`;
    const created = createEvent('BillingAccountCreated', {
      correlationId: id,
      customerId: e.customerId,
      source: SOURCE,
      payload: { accountId, planId: e.payload.planId },
    });

    if (existing?.status === 'CANCEL_REQUESTED') {
      // Caso borde: la activación ya había fallado → se crea y se anula en el acto
      await this.accounts.updateOne(
        { activationId: id },
        {
          $set: {
            accountId,
            planId: e.payload.planId,
            status: 'CANCELLED',
            updatedAt: new Date(),
          },
        },
        { session: tx.session },
      );
      await tx.publish(created);
      await tx.publish(
        createEvent('BillingAccountCancelled', {
          correlationId: id,
          customerId: e.customerId,
          source: SOURCE,
          payload: { accountId, reason: existing.cancelReason ?? 'La activación ya había fallado' },
        }),
      );
      this.logger.warn(`${id}: cuenta ${accountId} creada y anulada en el acto (ActivationFailed llegó antes)`);
      return;
    }

    await this.accounts.insertOne(
      {
        activationId: id,
        customerId: e.customerId,
        planId: e.payload.planId,
        accountId,
        status: 'CREATED',
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      { session: tx.session },
    );
    await tx.publish(created);
    this.logger.log(`${id}: cuenta ${accountId} creada`);
  }

  /** ActivationFailed → compensación: anula la cuenta si la había creado. */
  private async compensate(e: ActivationFailed, tx: TxContext): Promise<void> {
    const id = e.correlationId;
    const account = await this.accounts.findOne({ activationId: id }, { session: tx.session });

    if (!account) {
      // Todavía no procesé ActivationRequested: dejo anotado que hay que anular
      await this.accounts.insertOne(
        {
          activationId: id,
          customerId: e.customerId,
          status: 'CANCEL_REQUESTED',
          cancelReason: e.payload.reason,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
        { session: tx.session },
      );
      this.logger.warn(`${id}: ActivationFailed llegó antes que la cuenta → CANCEL_REQUESTED`);
      return;
    }

    if (account.status !== 'CREATED') {
      this.logger.log(`${id}: nada que compensar (cuenta en ${account.status})`);
      return;
    }

    await this.accounts.updateOne(
      { activationId: id, status: 'CREATED' },
      { $set: { status: 'CANCELLED', cancelReason: e.payload.reason, updatedAt: new Date() } },
      { session: tx.session },
    );
    await tx.publish(
      createEvent('BillingAccountCancelled', {
        correlationId: id,
        customerId: e.customerId,
        source: SOURCE,
        payload: { accountId: account.accountId!, reason: e.payload.reason },
      }),
    );
    this.logger.warn(`${id}: compensación → cuenta ${account.accountId} anulada`);
  }
}
