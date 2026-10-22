import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { BillingConsumer } from './billing.consumer';

@Module({
  imports: [KafkaToolkitModule.forRoot()],
  controllers: [BillingConsumer],
})
export class AppModule {}
