export * from './config';
export * from './mongo.service';
export * from './event-publisher';
export * from './outbox.service';
export * from './reliable-consumer';
export * from './toolkit.module';

/** Pausa simple (simula latencia de sistemas externos). */
export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
export const randomBetween = (min: number, max: number) => Math.floor(min + Math.random() * (max - min + 1));
