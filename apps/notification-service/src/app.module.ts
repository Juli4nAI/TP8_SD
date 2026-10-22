import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { NotificationConsumer } from './notification.consumer';
import { Mailer } from './mailer';

@Module({
  imports: [KafkaToolkitModule.forRoot()],
  controllers: [NotificationConsumer],
  providers: [Mailer],
})
export class AppModule {}
