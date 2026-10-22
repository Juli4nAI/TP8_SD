import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { ProvisioningConsumer } from './provisioning.consumer';

@Module({
  imports: [KafkaToolkitModule.forRoot()],
  controllers: [ProvisioningConsumer],
})
export class AppModule {}
