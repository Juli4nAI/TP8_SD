import { KafkaOptions, Transport } from '@nestjs/microservices';

/** Variables de entorno comunes (sección 09 del documento). */
export const env = {
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((b) => b.trim()),
  clientId: process.env.KAFKA_CLIENT_ID ?? 'activation-poc',
  groupId: process.env.KAFKA_GROUP_ID ?? 'activation-poc',
  mongoUri: process.env.MONGO_URI ?? 'mongodb://localhost:27017/activation-poc?replicaSet=rs0&directConnection=true',
};

export interface ConsumerOptions {
  clientId?: string;
  groupId?: string;
  /** Solo aplica a un consumer group NUEVO (sin offsets confirmados). */
  fromBeginning?: boolean;
}

/**
 * Opciones del transporte Kafka de NestJS para el lado CONSUMIDOR (@EventPattern).
 *
 *  - postfixId: ''        → Nest no le agrega "-server" al groupId/clientId.
 *  - autoCommit: false    → el offset se confirma a mano, DESPUÉS de procesar (ReliableConsumer).
 *  - fromBeginning: true  → un grupo nuevo lee el historial completo (no se pierde nada al arrancar).
 *  - sessionTimeout 15 s  → si una instancia muere, el grupo rebalancea en ~15 s (escenario 4).
 */
export function kafkaConsumerOptions(opts: ConsumerOptions = {}): KafkaOptions {
  return {
    transport: Transport.KAFKA,
    options: {
      postfixId: '',
      client: {
        clientId: opts.clientId ?? env.clientId,
        brokers: env.brokers,
        retry: { initialRetryTime: 300, retries: 15 },
      },
      consumer: {
        groupId: opts.groupId ?? env.groupId,
        sessionTimeout: 15000,
        heartbeatInterval: 3000,
        allowAutoTopicCreation: false,
      },
      subscribe: { fromBeginning: opts.fromBeginning ?? true },
      run: { autoCommit: false },
    },
  };
}
