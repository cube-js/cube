import { v4 as uuidv4 } from 'uuid';
import type { ZodError } from 'zod';

import { UserError } from '../user-error';
import { ExtendedRequestContext, ContextAcceptorFn } from '../interfaces';
import { CubejsHandlerError } from '../cubejs-handler-error';
import {
  authMessageSchema,
  unsubscribeMessageSchema,
  methodMessageSchema,
  WsMessage,
} from './message-schema';

import type { ApiGateway } from '../gateway';
import type { LocalSubscriptionStore } from './local-subscription-store';

const methodParams: Record<string, string[]> = Object.freeze({
  load: ['query', 'queryType'],
  sql: ['query'],
  'dry-run': ['query'],
  meta: [],
  subscribe: ['query', 'queryType'],
  unsubscribe: [],
});

const calcMessageLength = (message: unknown) => Buffer.byteLength(
  typeof message === 'string' ? message : JSON.stringify(message)
);

export type WebSocketSendMessageFn = (connectionId: string, message: any) => Promise<void>;

export class SubscriptionServer {
  public constructor(
    protected readonly apiGateway: ApiGateway,
    protected readonly sendMessage: WebSocketSendMessageFn,
    protected readonly subscriptionStore: LocalSubscriptionStore,
    protected readonly contextAcceptor: ContextAcceptorFn,
  ) {
  }

  protected resultFn(connectionId: string, messageId: string | number | undefined, requestId: string | undefined, logNetworkUsage: boolean = true) {
    return async (message, { status } = { status: 200 }) => {
      if (logNetworkUsage) {
        this.apiGateway.log({ type: 'Outgoing network usage', service: 'api-ws', bytes: calcMessageLength(message), }, { requestId });
      }

      return this.sendMessage(connectionId, { messageId, message, status });
    };
  }

  protected deserializeMessage(message: any): any {
    try {
      return JSON.parse(message);
    } catch (e: any) {
      throw new CubejsHandlerError(400, 'Invalid JSON payload', e.message);
    }
  }

  protected mapZodError(error: ZodError): string {
    return error.issues
      .map(e => (e.path.length ? `${e.path.join('.')}: ${e.message}` : e.message))
      .join(', ');
  }

  protected validateMessage(message: object): WsMessage {
    if ('authorization' in message) {
      const result = authMessageSchema.safeParse(message);
      if (!result.success) {
        throw new CubejsHandlerError(400, 'Invalid authorization message format', this.mapZodError(result.error));
      }

      return result.data;
    }

    if ('unsubscribe' in message) {
      const result = unsubscribeMessageSchema.safeParse(message);
      if (!result.success) {
        throw new CubejsHandlerError(400, 'Invalid unsubscribe message format', this.mapZodError(result.error));
      }

      return result.data;
    }

    const result = methodMessageSchema.safeParse(message);
    if (!result.success) {
      throw new CubejsHandlerError(400, 'Invalid message format', this.mapZodError(result.error));
    }

    return result.data;
  }

  public async processMessage(connectionId: string, body: string) {
    let message: any | undefined;

    try {
      message = this.deserializeMessage(body);
      message = this.validateMessage(message);

      await this.handleMessage(connectionId, message, false);
    } catch (e) {
      this.apiGateway.handleError({
        e,
        query: message?.query,
        res: this.resultFn(connectionId, message?.messageId, undefined, false),
      });
    }
  }

  protected async handleMessage(connectionId: string, message: WsMessage, isSubscription: boolean) {
    let authContext: any = {};
    let context: Partial<ExtendedRequestContext> = {};

    const bytes = calcMessageLength(message);

    try {
      if ('authorization' in message) {
        authContext = { isSubscription, protocol: 'ws' };
        await this.apiGateway.checkAuthFn(authContext, message.authorization);

        const acceptanceResult = await this.contextAcceptor(authContext);
        if (!acceptanceResult.accepted) {
          this.sendMessage(connectionId, acceptanceResult.rejectMessage);
          return;
        }

        await this.subscriptionStore.setAuthContext(connectionId, authContext);
        this.sendMessage(connectionId, { handshake: true });
        return;
      }

      if ('unsubscribe' in message) {
        await this.subscriptionStore.unsubscribe(connectionId, message.unsubscribe);
        return;
      }

      if (!message.messageId) {
        throw new UserError('messageId is required');
      }

      authContext = await this.subscriptionStore.getAuthContext(connectionId);
      if (!authContext) {
        await this.sendMessage(
          connectionId,
          {
            messageId: message.messageId,
            message: { error: 'Not authorized' },
            status: 403
          }
        );
        return;
      }

      if (!message.method) {
        throw new UserError('Method is required');
      }

      if (!methodParams.hasOwnProperty(message.method)) {
        throw new UserError(`Unsupported method: ${message.method}`);
      }

      const subscriptionId = String(message.messageId);
      const baseRequestId = message.requestId || `${connectionId}-${subscriptionId}`;
      const requestId = `${baseRequestId}-span-${uuidv4()}`;

      context = await this.apiGateway.contextByReq(
        // TODO: We need to standardize type for WS request type
        message as any,
        authContext.securityContext,
        requestId
      );

      this.apiGateway.log({
        type: 'Incoming network usage',
        service: 'api-ws',
        bytes,
      }, context);

      const collectedParams: Record<string, unknown> = Object.create(null);

      if (message.params) {
        for (const k of methodParams[message.method]) {
          collectedParams[k] = message.params[k];
        }
      }

      const method = message.method.replace(/[^a-z]+(.)/g, (_m, chr) => chr.toUpperCase());
      await this.apiGateway[method]({
        ...collectedParams,
        connectionId,
        context,
        signedWithPlaygroundAuthSecret: authContext.signedWithPlaygroundAuthSecret,
        isSubscription,
        apiType: 'ws',
        res: this.resultFn(connectionId, message.messageId, requestId),
        subscriptionState: async () => {
          const subscription = await this.subscriptionStore.getSubscription(connectionId, subscriptionId);
          return subscription && subscription.state;
        },
        subscribe: async (state) => this.subscriptionStore.subscribe(connectionId, subscriptionId, {
          message,
          state
        }),
        unsubscribe: async () => this.subscriptionStore.unsubscribe(connectionId, subscriptionId)
      });

      await this.sendMessage(connectionId, { messageProcessedId: message.messageId });
    } catch (e) {
      const messageId = 'messageId' in message ? message.messageId : undefined;
      const query = 'params' in message ? message.params?.query : undefined;

      this.apiGateway.handleError({
        e,
        query,
        res: this.resultFn(connectionId, messageId, context.requestId),
        context
      });
    }
  }

  public async processSubscriptions() {
    const allSubscriptions = this.subscriptionStore.getAllSubscriptions();
    await Promise.all(allSubscriptions.map(async subscription => {
      await this.handleMessage(subscription.connectionId, subscription.message, true);
    }));
  }

  public async disconnect(connectionId: string) {
    await this.subscriptionStore.disconnect(connectionId);
  }

  public clear() {
    this.subscriptionStore.clear();
  }
}
