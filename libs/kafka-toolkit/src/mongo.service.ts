import { Logger, OnApplicationShutdown } from '@nestjs/common';
import { ClientSession, Db, MongoClient } from 'mongodb';

export const PROCESSED_EVENTS = 'processed_events';
export const OUTBOX = 'outbox';

/** 7 días: igual que la retención de los topics. Un evento más viejo ya no puede volver a llegar. */
export const SEVEN_DAYS_SECONDS = 7 * 24 * 60 * 60;

/**
 * Conexión a MongoDB (una base por servicio, definida en MONGO_URI).
 * Mongo corre como replica set de 1 nodo para poder usar transacciones.
 */
export class MongoService implements OnApplicationShutdown {
  private readonly logger = new Logger('MongoService');
  readonly client: MongoClient;
  db: Db;

  constructor(private readonly uri: string) {
    this.client = new MongoClient(uri, { serverSelectionTimeoutMS: 30000 });
  }

  async connect(): Promise<void> {
    await this.client.connect();
    this.db = this.client.db(); // la base viene en la URI
    await this.ensureCommonIndexes();
    this.logger.log(`Conectado a MongoDB (base "${this.db.databaseName}")`);
  }

  private async ensureCommonIndexes(): Promise<void> {
    // processed_events: _id = eventId (único por definición) + TTL de 7 días
    await this.db
      .collection(PROCESSED_EVENTS)
      .createIndex({ processedAt: 1 }, { expireAfterSeconds: SEVEN_DAYS_SECONDS, name: 'ttl_7d' });
    // outbox: la consulta del relay es por status + createdAt
    await this.db.collection(OUTBOX).createIndex({ status: 1, createdAt: 1 }, { name: 'status_createdAt' });
    await this.db.collection(OUTBOX).createIndex({ correlationId: 1 }, { name: 'correlationId' });
    await this.db
      .collection(OUTBOX)
      .createIndex({ sentAt: 1 }, { expireAfterSeconds: SEVEN_DAYS_SECONDS, name: 'ttl_sent_7d' });
  }

  /** Ejecuta `fn` dentro de una transacción (con los reintentos automáticos del driver). */
  async withTransaction<T>(fn: (session: ClientSession) => Promise<T>): Promise<T> {
    const session = this.client.startSession();
    try {
      let result!: T;
      await session.withTransaction(async () => {
        result = await fn(session);
      });
      return result;
    } finally {
      await session.endSession();
    }
  }

  async onApplicationShutdown(): Promise<void> {
    await this.client.close().catch(() => undefined);
  }
}

export function isDuplicateKeyError(err: unknown): boolean {
  const e = err as { code?: number; errorResponse?: { code?: number } } | undefined;
  return e?.code === 11000 || e?.errorResponse?.code === 11000;
}
