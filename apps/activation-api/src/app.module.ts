import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { ActivationsController } from './activations.controller';
import { ActivationsGateway } from './activations.gateway';
import { ActivationsService } from './activations.service';
import { SagaTimeoutService } from './saga-timeout.service';
import { SagaConsumer } from './saga.consumer';

@Module({
  imports: [KafkaToolkitModule.forRoot()],
  controllers: [ActivationsController, SagaConsumer],
  providers: [ActivationsService, ActivationsGateway, SagaTimeoutService],
})
export class AppModule {}
