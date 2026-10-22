import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';

export const KAFKA_PRODUCER = 'KAFKA_PRODUCER';

export interface RawMessage {
  topic: string;
  key: string | null;
  value: string;
  headers?: Record<string, string>;
}

/**
 * Publicación en Kafka. Usa ClientKafka de @nestjs/microservices (en modo "solo productor")
 * y lo aísla detrás de esta clase: si la POC pasa a piloto, cambiar kafkajs por otro
 * cliente toca solo este archivo (riesgo "mantenimiento limitado de kafkajs").
 */
@Injectable()
export class EventPublisher implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger('EventPublisher');

  constructor(@Inject(KAFKA_PRODUCER) private readonly client: ClientKafka) {}

  async onModuleInit(): Promise<void> {
    await this.client.connect();
    this.logger.log('Productor Kafka conectado');
  }

  async publish(msg: RawMessage): Promise<void> {
    await lastValueFrom(
      this.client.emit(msg.topic, {
        key: msg.key,
        value: msg.value,
        headers: msg.headers ?? {},
      }),
    );
  }

  async onApplicationShutdown(): Promise<void> {
    await this.client.close().catch(() => undefined);
  }
}
