import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { kafkaConsumerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

/**
 * activation-api es una app HÍBRIDA: HTTP (REST) + WebSocket + consumidor Kafka.
 * Es el único punto de entrada; todo lo demás reacciona a eventos.
 */
async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors({ origin: true });
  app.enableShutdownHooks();

  app.connectMicroservice(kafkaConsumerOptions(), { inheritAppConfig: true });
  await app.startAllMicroservices();

  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port, '0.0.0.0');
  new Logger('activation-api').log(`Escuchando en http://localhost:${port}`);
}

bootstrap().catch((err) => {
  console.error(err);
  process.exit(1);
});
