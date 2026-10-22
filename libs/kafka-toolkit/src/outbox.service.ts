import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { DomainEvent, HEADERS, TOPIC_BY_EVENT } from '@poc/contracts';
import { randomUUID } from 'crypto';
import { ClientSession, Collection } from 'mongodb';
import { EventPublisher } from './event-publisher';
import { MongoService, OUTBOX } from './mongo.service';

export type OutboxStatus = 'PENDING' | 'PUBLISHING' | 'SENT';

export interface OutboxDoc {
  _id: string; // = eventId
  topic: string;
  key: string;
  value: string; // evento serializado
  headers: Record<string, string>;
  eventType: string;
  correlationId: string;
  status: OutboxStatus;
  attempts: number;
  createdAt: Date;
  lockedUntil?: Date;
  lockedBy?: string;
  sentAt?: Date;
  lastError?: string;
}

const POLL_MS = Number(process.env.OUTBOX_POLL_MS ?? 500);
const LOCK_MS = 15000;
const BATCH = 100;

/**
 * Outbox transaccional (decisión técnica 3).
 *
 * El evento se guarda en la colección `outbox` EN LA MISMA TRANSACCIÓN que el cambio de estado.
 * Un relay lo publica después (cada 500 ms, o enseguida con kick()). Si el servicio se cae
 * entre guardar y publicar, el evento sigue en `outbox` y se publica al volver: no se pierde.
 *
 * Si se cae entre publicar y marcar SENT, se publica dos veces → no pasa nada porque los
 * consumidores son idempotentes por eventId. Outbox e idempotencia se necesitan mutuamente.
 */
@Injectable()
export class OutboxService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger('Outbox');
  private readonly instanceId = randomUUID().slice(0, 8);
  private timer?: NodeJS.Timeout;
  private running = false;
  private again = false;

  constructor(
    private readonly mongo: MongoService,
    private readonly publisher: EventPublisher,
  ) {}

  private get col(): Collection<OutboxDoc> {
    return this.mongo.db.collection<OutboxDoc>(OUTBOX);
  }

  /** Agrega un evento al outbox dentro de la transacción en curso. */
  async add(event: DomainEvent, session: ClientSession): Promise<void> {
    const doc: OutboxDoc = {
      _id: event.eventId,
      topic: TOPIC_BY_EVENT[event.eventType],
      key: event.customerId,
      value: JSON.stringify(event),
      headers: {
        [HEADERS.CORRELATION_ID]: event.correlationId,
        [HEADERS.EVENT_TYPE]: event.eventType,
        [HEADERS.EVENT_ID]: event.eventId,
        [HEADERS.SOURCE]: event.source,
      },
      eventType: event.eventType,
      correlationId: event.correlationId,
      status: 'PENDING',
      attempts: 0,
      createdAt: new Date(),
    };
    // upsert + $setOnInsert: si el mismo evento (mismo eventId) ya estaba, no se duplica
    const { _id, ...rest } = doc;
    await this.col.updateOne({ _id }, { $setOnInsert: rest }, { upsert: true, session });
  }

  /**
   * Vuelve a poner en PENDING los eventos ya enviados de una activación.
   * Se usa en la demo para probar la idempotencia: se reenvían con EL MISMO eventId.
   */
  async resend(correlationId: string, eventTypes: string[]): Promise<number> {
    const res = await this.col.updateMany(
      { correlationId, eventType: { $in: eventTypes }, status: 'SENT' },
      { $set: { status: 'PENDING' }, $unset: { sentAt: '', lockedUntil: '', lockedBy: '' } },
    );
    this.kick();
    return res.modifiedCount;
  }

  /** Pide publicar ya (sin esperar al próximo ciclo de 500 ms). */
  kick(): void {
    setImmediate(() => void this.flush());
  }

  onApplicationBootstrap(): void {
    this.timer = setInterval(() => void this.flush(), POLL_MS);
    this.kick();
  }

  onApplicationShutdown(): void {
    if (this.timer) clearInterval(this.timer);
  }

  /** Publica los pendientes en orden de creación (mantiene el orden por cliente). */
  async flush(): Promise<void> {
    if (this.running) {
      this.again = true;
      return;
    }
    this.running = true;
    try {
      do {
        this.again = false;
        for (let i = 0; i < BATCH; i++) {
          const doc = await this.claimNext();
          if (!doc) break;
          const ok = await this.publishOne(doc);
          if (!ok) break; // Kafka no disponible: se reintenta en el próximo ciclo
        }
      } while (this.again);
    } catch (err) {
      this.logger.warn(`Error en el relay del outbox: ${(err as Error).message}`);
    } finally {
      this.running = false;
    }
  }

  /** Toma el próximo pendiente con un "lease", así dos instancias no publican el mismo a la vez. */
  private async claimNext(): Promise<OutboxDoc | null> {
    const now = new Date();
    return this.col.findOneAndUpdate(
      {
        $or: [{ status: 'PENDING' }, { status: 'PUBLISHING', lockedUntil: { $lt: now } }],
      },
      {
        $set: { status: 'PUBLISHING', lockedUntil: new Date(now.getTime() + LOCK_MS), lockedBy: this.instanceId },
        $inc: { attempts: 1 },
      },
      { sort: { createdAt: 1 }, returnDocument: 'after' },
    );
  }

  private async publishOne(doc: OutboxDoc): Promise<boolean> {
    try {
      await this.publisher.publish({ topic: doc.topic, key: doc.key, value: doc.value, headers: doc.headers });
      await this.col.updateOne(
        { _id: doc._id, lockedBy: this.instanceId },
        { $set: { status: 'SENT', sentAt: new Date() }, $unset: { lockedUntil: '', lockedBy: '', lastError: '' } },
      );
      this.logger.log(`→ ${doc.topic} ${doc.eventType} [${doc.correlationId}]`);
      return true;
    } catch (err) {
      await this.col.updateOne(
        { _id: doc._id },
        { $set: { status: 'PENDING', lastError: (err as Error).message }, $unset: { lockedUntil: '', lockedBy: '' } },
      );
      this.logger.warn(`No se pudo publicar ${doc.eventType} [${doc.correlationId}]: ${(err as Error).message}`);
      return false;
    }
  }
}
