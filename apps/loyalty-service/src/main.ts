import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { kafkaConsumerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

/** loyalty-service (escenario 5): grupo NUEVO que lee el historial completo desde el inicio. */
async function bootstrap() {
  const app = await NestFactory.createMicroservice(AppModule, kafkaConsumerOptions());
  app.enableShutdownHooks();
  await app.listen();
  new Logger('loyalty-service').log(`Consumiendo como grupo "${process.env.KAFKA_GROUP_ID}"`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
