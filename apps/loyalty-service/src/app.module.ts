import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { LoyaltyConsumer } from './loyalty.consumer';

@Module({
  imports: [KafkaToolkitModule.forRoot()],
  controllers: [LoyaltyConsumer],
})
export class AppModule {}
