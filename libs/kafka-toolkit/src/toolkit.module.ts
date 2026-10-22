import { DynamicModule, Global, Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { env } from './config';
import { EventPublisher, KAFKA_PRODUCER } from './event-publisher';
import { MongoService } from './mongo.service';
import { OutboxService } from './outbox.service';
import { ReliableConsumer } from './reliable-consumer';

/**
 * Módulo que importa cada servicio: Mongo, productor Kafka, outbox y consumidor confiable.
 */
@Global()
@Module({})
export class KafkaToolkitModule {
  static forRoot(): DynamicModule {
    return {
      module: KafkaToolkitModule,
      imports: [
        ClientsModule.register([
          {
            name: KAFKA_PRODUCER,
            transport: Transport.KAFKA,
            options: {
              postfixId: '',
              client: {
                clientId: `${env.clientId}-producer`,
                brokers: env.brokers,
                retry: { initialRetryTime: 300, retries: 15 },
              },
              // Solo productor: sin esto Nest crea un consumer group extra "<grupo>-client"
              producerOnlyMode: true,
              producer: { allowAutoTopicCreation: false },
            },
          },
        ]),
      ],
      providers: [
        {
          provide: MongoService,
          useFactory: async () => {
            const mongo = new MongoService(env.mongoUri);
            await mongo.connect();
            return mongo;
          },
        },
        EventPublisher,
        OutboxService,
        ReliableConsumer,
      ],
      exports: [MongoService, EventPublisher, OutboxService, ReliableConsumer],
    };
  }
}
