import { Controller, Logger, OnModuleInit } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { ActivationRequested, createEvent, TOPICS } from '@poc/contracts';
import { MongoService, randomBetween, ReliableConsumer, sleep, TxContext } from '@poc/kafka-toolkit';
import { randomUUID } from 'crypto';
import { Collection } from 'mongodb';

const SOURCE = 'provisioning';

interface ProvisioningOrder {
  activationId: string;
  customerId: string;
  planId: string;
  orderId: string;
  status: 'COMPLETED' | 'FAILED';
  lineNumber?: string;
  reason?: string;
  createdAt: Date;
}

/** Consumer group "provisioning-svc": activation.requested */
@Controller()
export class ProvisioningConsumer implements OnModuleInit {
  private readonly logger = new Logger('Provisioning');

  constructor(
    private readonly consumer: ReliableConsumer,
    private readonly mongo: MongoService,
  ) {}

  private get orders(): Collection<ProvisioningOrder> {
    return this.mongo.db.collection<ProvisioningOrder>('provisioning_orders');
  }

  async onModuleInit(): Promise<void> {
    await this.orders.createIndex({ activationId: 1 }, { unique: true, name: 'activationId_unique' });
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Ctx() ctx: KafkaContext) {
    return this.consumer.process<ActivationRequested>(ctx, (e, tx) => this.provision(e, tx));
  }

  private async provision(e: ActivationRequested, tx: TxContext): Promise<void> {
    const id = e.correlationId;

    if (e.payload.simulateCrash === 'provisioning') {
      throw new Error('Simulado: el sistema de red no responde');
    }

    const existing = await this.orders.findOne({ activationId: id }, { session: tx.session });
    if (existing) {
      this.logger.log(`${id}: la orden ya existía (${existing.status}), no se duplica`);
      return;
    }

    await sleep(randomBetween(1000, 3000)); // aprovisionar la línea tarda de 1 a 3 s

    const orderId = `ORD-${randomUUID().slice(0, 8).toUpperCase()}`;

    if (e.payload.simulateFailure === 'provisioning') {
      const reason = 'Simulado: no hay numeración disponible en la zona';
      await this.orders.insertOne(
        { activationId: id, customerId: e.customerId, planId: e.payload.planId, orderId, status: 'FAILED', reason, createdAt: new Date() },
        { session: tx.session },
      );
      await tx.publish(
        createEvent('ProvisioningFailed', { correlationId: id, customerId: e.customerId, source: SOURCE, payload: { reason } }),
      );
      this.logger.warn(`${id}: ProvisioningFailed (simulado)`);
      return;
    }

    const lineNumber = `+54 9 3442 ${randomBetween(400000, 699999)}`;
    await this.orders.insertOne(
      { activationId: id, customerId: e.customerId, planId: e.payload.planId, orderId, status: 'COMPLETED', lineNumber, createdAt: new Date() },
      { session: tx.session },
    );
    await tx.publish(
      createEvent('ProvisioningCompleted', {
        correlationId: id,
        customerId: e.customerId,
        source: SOURCE,
        payload: { orderId, lineNumber },
      }),
    );
    this.logger.log(`${id}: línea ${lineNumber} aprovisionada`);
  }
}
