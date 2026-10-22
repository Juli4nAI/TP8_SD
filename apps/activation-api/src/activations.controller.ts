import { Body, Controller, Get, HttpCode, Param, Post, Query } from '@nestjs/common';
import { CreateActivationDto } from './activation.model';
import { ActivationsGateway } from './activations.gateway';
import { ActivationsService } from './activations.service';

@Controller()
export class ActivationsController {
  constructor(
    private readonly activations: ActivationsService,
    private readonly gateway: ActivationsGateway,
  ) {}

  @Get('health')
  health() {
    return { status: 'ok' };
  }

  /** RF-02: responde 202 Accepted enseguida, sin esperar a los demás servicios. */
  @Post('activations')
  @HttpCode(202)
  async create(@Body() dto: CreateActivationDto) {
    const { activationId, status, doc } = await this.activations.create(dto);
    this.gateway.emitUpdate(doc);
    return { activationId, status };
  }

  /** Lista las últimas activaciones (para la UI). */
  @Get('activations')
  list(@Query('limit') limit?: string) {
    return this.activations.list(Number(limit) || 20);
  }

  /** RF-09: estado actual + historial de eventos. */
  @Get('activations/:id')
  get(@Param('id') id: string) {
    return this.activations.get(id);
  }

  /** Demo de idempotencia: reenvía los eventos de la activación con el mismo eventId. */
  @Post('activations/:id/resend')
  @HttpCode(202)
  async resend(@Param('id') id: string) {
    const { resent, doc } = await this.activations.resend(id);
    this.gateway.emitUpdate(doc);
    return { activationId: id, resent };
  }
}
