import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { ActivationOutcomeEvent, TOPICS } from '@poc/contracts';
import { MongoService, ReliableConsumer } from '@poc/kafka-toolkit';
import { Collection } from 'mongodb';

const POINTS_PER_ACTIVATION = 100;

interface LoyaltyAccount {
  _id: string; // customerId
  points: number;
  activations: string[];
  updatedAt: Date;
}

/**
 * Escenario 5 — "Nuevo consumidor": un servicio que no existía cuando ocurrieron los eventos.
 * Se suscribe con un consumer group NUEVO ("loyalty") y fromBeginning (equivalente a
 * auto.offset.reset=earliest), así que procesa TODO el historial de activation.events
 * sin tocar a los demás servicios. Esto con APIs directas no se puede hacer.
 */
@Controller()
export class LoyaltyConsumer {
  private readonly logger = new Logger('Loyalty');

  constructor(
    private readonly consumer: ReliableConsumer,
    private readonly mongo: MongoService,
  ) {}

  private get accounts(): Collection<LoyaltyAccount> {
    return this.mongo.db.collection<LoyaltyAccount>('loyalty_accounts');
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationOutcome(@Ctx() ctx: KafkaContext) {
    return this.consumer.process<ActivationOutcomeEvent>(ctx, async (e, tx) => {
      if (e.eventType !== 'ActivationCompleted') return;
      const res = await this.accounts.findOneAndUpdate(
        { _id: e.customerId },
        {
          $inc: { points: POINTS_PER_ACTIVATION },
          $push: { activations: e.correlationId },
          $set: { updatedAt: new Date() },
        },
        { upsert: true, returnDocument: 'after', session: tx.session },
      );
      this.logger.log(
        `+${POINTS_PER_ACTIVATION} puntos a ${e.customerId} por ${e.correlationId} (total: ${res?.points}) ← offset ${ctx.getMessage().offset}, evento del ${e.occurredAt}`,
      );
    });
  }
}
