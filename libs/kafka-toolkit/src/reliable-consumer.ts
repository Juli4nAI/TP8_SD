import { Injectable, Logger } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import { assertEnvelope, dlqOf, DomainEvent, HEADERS } from '@poc/contracts';
import { ClientSession } from 'mongodb';
import { env } from './config';
import { EventPublisher } from './event-publisher';
import { isDuplicateKeyError, MongoService, PROCESSED_EVENTS } from './mongo.service';
import { OutboxService } from './outbox.service';

/** Esperas entre reintentos (decisión técnica 2): 1 intento + 3 reintentos = 4 intentos. */
export const RETRY_DELAYS_MS = [1000, 2000, 4000];

/** Lo que recibe el handler de negocio: la sesión de la transacción y cómo publicar. */
export interface TxContext {
  session: ClientSession;
  /** Agrega el evento al outbox en la misma transacción (se publica al confirmar). */
  publish(event: DomainEvent): Promise<void>;
}

export type EventHandler<E extends DomainEvent> = (event: E, tx: TxContext) => Promise<void>;

class DuplicateEventError extends Error {}

/**
 * Envoltorio que usan TODOS los consumidores. Junta en un solo lugar:
 *
 *  1. Idempotencia: inserta el eventId en `processed_events` en la MISMA transacción que el
 *     efecto. Si ya estaba (índice único), descarta el evento.
 *  2. Reintentos: ante un error técnico reintenta con espera creciente (1 s, 2 s, 4 s).
 *  3. DLQ: si sigue fallando, publica el mensaje original en `<topic>.dlq` con el motivo en
 *     los headers y sigue con el siguiente (no bloquea la partición).
 *  4. Commit manual del offset: siempre DESPUÉS de procesar, nunca antes (at-least-once).
 */
@Injectable()
export class ReliableConsumer {
  private readonly logger = new Logger('ReliableConsumer');

  constructor(
    private readonly mongo: MongoService,
    private readonly outbox: OutboxService,
    private readonly publisher: EventPublisher,
  ) {}

  async process<E extends DomainEvent = DomainEvent>(ctx: KafkaContext, handler: EventHandler<E>): Promise<void> {
    const message = ctx.getMessage();
    const topic = ctx.getTopic();
    const partition = ctx.getPartition();
    const where = `${topic}[p${partition}@${message.offset}]`;

    for (let attempt = 1; ; attempt++) {
      try {
        const event = this.decode(message.value);
        const result = await this.runInTransaction(event as E, handler);
        if (result === 'duplicate') {
          this.logger.log(`↺ ${where} ${event.eventType} ${event.eventId} ya procesado → descartado (idempotencia)`);
        }
        break;
      } catch (err) {
        const reason = (err as Error)?.message ?? String(err);
        if (attempt > RETRY_DELAYS_MS.length) {
          await this.sendToDlq(ctx, reason, attempt);
          break;
        }
        const wait = RETRY_DELAYS_MS[attempt - 1];
        this.logger.warn(`✗ ${where} intento ${attempt} falló: ${reason} → reintento en ${wait / 1000} s`);
        await this.sleepWithHeartbeat(ctx, wait);
      }
    }

    await this.commit(ctx);
  }

  // ---------------------------------------------------------------- internos

  private decode(value: unknown): DomainEvent {
    let parsed: unknown = value;
    if (Buffer.isBuffer(value)) parsed = value.toString('utf8');
    if (typeof parsed === 'string') {
      try {
        parsed = JSON.parse(parsed);
      } catch {
        throw new Error('Mensaje inválido: el valor no es JSON');
      }
    }
    assertEnvelope(parsed);
    return parsed;
  }

  private async runInTransaction<E extends DomainEvent>(
    event: E,
    handler: EventHandler<E>,
  ): Promise<'processed' | 'duplicate'> {
    let wroteOutbox = false;
    try {
      await this.mongo.withTransaction(async (session) => {
        try {
          await this.mongo.db
            .collection<{ _id: string }>(PROCESSED_EVENTS)
            .insertOne(
              { _id: event.eventId, eventType: event.eventType, correlationId: event.correlationId, processedAt: new Date() } as never,
              { session },
            );
        } catch (err) {
          if (isDuplicateKeyError(err)) throw new DuplicateEventError();
          throw err;
        }
        const tx: TxContext = {
          session,
          publish: async (out) => {
            await this.outbox.add(out, session);
            wroteOutbox = true;
          },
        };
        await handler(event, tx);
      });
    } catch (err) {
      if (err instanceof DuplicateEventError) return 'duplicate';
      throw err;
    }
    if (wroteOutbox) this.outbox.kick();
    return 'processed';
  }

  private async sendToDlq(ctx: KafkaContext, reason: string, attempts: number): Promise<void> {
    const message = ctx.getMessage();
    const topic = ctx.getTopic();
    const value = message.value;
    const raw =
      value == null
        ? ''
        : Buffer.isBuffer(value)
          ? value.toString('utf8')
          : typeof value === 'string'
            ? value
            : JSON.stringify(value);

    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(message.headers ?? {})) {
      if (v != null) headers[k] = Buffer.isBuffer(v) ? v.toString('utf8') : String(v);
    }
    Object.assign(headers, {
      [HEADERS.ERROR]: reason.slice(0, 500),
      [HEADERS.ORIGINAL_TOPIC]: topic,
      [HEADERS.ORIGINAL_PARTITION]: String(ctx.getPartition()),
      [HEADERS.ORIGINAL_OFFSET]: String(message.offset),
      [HEADERS.CONSUMER_GROUP]: env.groupId,
      [HEADERS.ATTEMPTS]: String(attempts),
    });

    const key = message.key == null ? null : Buffer.isBuffer(message.key) ? message.key.toString('utf8') : String(message.key);
    await this.publisher.publish({ topic: dlqOf(topic), key, value: raw, headers });
    this.logger.error(
      `☠ ${topic}[p${ctx.getPartition()}@${message.offset}] falló ${attempts} veces → enviado a ${dlqOf(topic)} (${reason})`,
    );
  }

  private async commit(ctx: KafkaContext): Promise<void> {
    const message = ctx.getMessage();
    try {
      // Se confirma el SIGUIENTE offset: "ya procesé hasta acá inclusive"
      await ctx.getConsumer().commitOffsets([
        { topic: ctx.getTopic(), partition: ctx.getPartition(), offset: (BigInt(message.offset) + 1n).toString() },
      ]);
    } catch (err) {
      // Ej.: rebalanceo en curso. El mensaje se va a volver a entregar y la idempotencia lo descarta.
      this.logger.warn(`No se pudo confirmar el offset ${message.offset}: ${(err as Error).message}`);
    }
  }

  /** Espera sin que el broker crea que el consumidor murió (manda heartbeats). */
  private async sleepWithHeartbeat(ctx: KafkaContext, ms: number): Promise<void> {
    const heartbeat = ctx.getHeartbeat?.();
    const end = Date.now() + ms;
    while (Date.now() < end) {
      await new Promise((r) => setTimeout(r, Math.min(1000, end - Date.now())));
      await heartbeat?.().catch(() => undefined);
    }
  }
}
