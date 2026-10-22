import { Logger } from '@nestjs/common';
import { OnGatewayConnection, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { ActivationDoc } from './activation.model';

/**
 * Empuja cada cambio de una activación a la UI (RF-08).
 * La UI recibe `activation:update` con el documento completo y redibuja la timeline.
 */
@WebSocketGateway({ cors: { origin: '*' } })
export class ActivationsGateway implements OnGatewayConnection {
  private readonly logger = new Logger('WebSocket');

  @WebSocketServer()
  server: Server;

  handleConnection(client: Socket): void {
    this.logger.log(`UI conectada (${client.id})`);
  }

  emitUpdate(doc: ActivationDoc | null): void {
    if (doc) this.server?.emit('activation:update', doc);
  }
}
