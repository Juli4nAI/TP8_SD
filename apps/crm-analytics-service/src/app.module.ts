import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { CrmConsumer } from './crm.consumer';

@Module({
  imports: [KafkaToolkitModule.forRoot()],
  controllers: [CrmConsumer],
})
export class AppModule {}
