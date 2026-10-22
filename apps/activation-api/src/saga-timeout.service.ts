import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { OPEN_STATUSES } from './activation.model';
import { ActivationsGateway } from './activations.gateway';
import { ActivationsService } from './activations.service';

const TIMEOUT_MS = Number(process.env.SAGA_TIMEOUT_MS ?? 30000);
const SWEEP_MS = 5000;

/**
 * Cierra la saga si un resultado nunca llega (p. ej. el mensaje terminó en la DLQ):
 * la activación pasa a FAILED por timeout y se publica ActivationFailed, que dispara la
 * compensación igual que un fallo normal. Sin esto la activación quedaría colgada.
 */
@Injectable()
export class SagaTimeoutService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger('SagaTimeout');
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(
    private readonly activations: ActivationsService,
    private readonly gateway: ActivationsGateway,
  ) {}

  onApplicationBootstrap(): void {
    this.timer = setInterval(() => void this.sweep(), SWEEP_MS);
    this.logger.log(`Timeout de la saga: ${TIMEOUT_MS / 1000} s`);
  }

  onApplicationShutdown(): void {
    if (this.timer) clearInterval(this.timer);
  }

  private async sweep(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const limit = new Date(Date.now() - TIMEOUT_MS);
      const stale = await this.activations.col
        .find({ status: { $in: OPEN_STATUSES }, createdAt: { $lt: limit } })
        .limit(50)
        .toArray();
      for (const doc of stale) {
        if (await this.activations.expire(doc, TIMEOUT_MS)) {
          this.gateway.emitUpdate(await this.activations.findById(doc._id));
        }
      }
    } catch (err) {
      this.logger.warn(`Error revisando timeouts: ${(err as Error).message}`);
    } finally {
      this.running = false;
    }
  }
}
