import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { BillingEvent, ProvisioningEvent, TOPICS } from '@poc/contracts';
import { ReliableConsumer } from '@poc/kafka-toolkit';
import { ActivationsGateway } from './activations.gateway';
import { ActivationsService } from './activations.service';

/** Consume los resultados de billing y provisioning (consumer group "activation-api"). */
@Controller()
export class SagaConsumer {
  constructor(
    private readonly consumer: ReliableConsumer,
    private readonly activations: ActivationsService,
    private readonly gateway: ActivationsGateway,
  ) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Ctx() ctx: KafkaContext) {
    return this.handle(ctx);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Ctx() ctx: KafkaContext) {
    return this.handle(ctx);
  }

  private async handle(ctx: KafkaContext): Promise<void> {
    let touched = null as string | null;
    await this.consumer.process<BillingEvent | ProvisioningEvent>(ctx, async (event, tx) => {
      touched = event.correlationId;
      await this.activations.onStepResult(event, tx);
    });
    // Recién después del commit se avisa a la UI
    if (touched) this.gateway.emitUpdate(await this.activations.findById(touched));
  }
}
