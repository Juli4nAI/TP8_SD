import { Controller, Logger, OnModuleInit } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { DomainEvent, TOPICS } from '@poc/contracts';
import { MongoService, ReliableConsumer, TxContext } from '@poc/kafka-toolkit';
import { Collection } from 'mongodb';

interface EventLogDoc {
  _id: string; // = eventId (único)
  eventType: string;
  correlationId: string;
  customerId: string;
  source: string;
  occurredAt: string;
  topic: string;
  partition: number;
  offset: string;
  payload: unknown;
  receivedAt: Date;
}

/**
 * Consumer group "crm-analytics": escucha los 4 topics y NO publica nada.
 * Demuestra que se puede sumar un consumidor sin tocar a los demás (RF-10).
 * Para reprocesar el historial desde el inicio: scripts/replay-crm.(ps1|sh).
 */
@Controller()
export class CrmConsumer implements OnModuleInit {
  private readonly logger = new Logger('CRM');

  constructor(
    private readonly consumer: ReliableConsumer,
    private readonly mongo: MongoService,
  ) {}

  private get log(): Collection<EventLogDoc> {
    return this.mongo.db.collection<EventLogDoc>('event_log');
  }

  async onModuleInit(): Promise<void> {
    await this.log.createIndex({ correlationId: 1, occurredAt: 1 }, { name: 'correlationId' });
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onRequested(@Ctx() ctx: KafkaContext) {
    return this.handle(ctx);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Ctx() ctx: KafkaContext) {
    return this.handle(ctx);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Ctx() ctx: KafkaContext) {
    return this.handle(ctx);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivation(@Ctx() ctx: KafkaContext) {
    return this.handle(ctx);
  }

  private handle(ctx: KafkaContext) {
    return this.consumer.process<DomainEvent>(ctx, (e, tx) => this.record(e, tx, ctx));
  }

  private async record(e: DomainEvent, tx: TxContext, ctx: KafkaContext): Promise<void> {
    await this.log.insertOne(
      {
        _id: e.eventId,
        eventType: e.eventType,
        correlationId: e.correlationId,
        customerId: e.customerId,
        source: e.source,
        occurredAt: e.occurredAt,
        topic: ctx.getTopic(),
        partition: ctx.getPartition(),
        offset: ctx.getMessage().offset,
        payload: e.payload,
        receivedAt: new Date(),
      },
      { session: tx.session },
    );
    this.logger.log(`${e.eventType} [${e.correlationId}] ← ${ctx.getTopic()} p${ctx.getPartition()}@${ctx.getMessage().offset}`);
  }
}
