import { v4 as uuidv4 } from 'uuid';
import type { ZodError } from 'zod';
import { getEnv, isValidRequestId } from '@cubejs-backend/shared';

import { UserError } from '../user-error';
import { ExtendedRequestContext, ContextAcceptorFn } from '../interfaces';
import { CubejsHandlerError } from '../cubejs-handler-error';
import {
  authMessageSchema,
  unsubscribeMessageSchema,
  methodMessageSchema,
  validateMessageId,
  WsMessage,
} from './message-schema';

import type { ApiGateway } from '../gateway';
import type { LocalSubscriptionStore } from './local-subscription-store';

const methodParams: Record<string, string[]> = Object.freeze({
  load: ['query', 'queryType', 'cache'],
  sql: ['query'],
  'dry-run': ['query'],
  meta: ['onlyViews'],
  subscribe: ['query', 'queryType', 'cache'],
  unsubscribe: [],
});

const calcMessageLength = (message: unknown) => Buffer.byteLength(
  typeof message === 'string' ? message : JSON.stringify(message)
);

export type WebSocketSendMessageFn = (connectionId: string, message: any) => Promise<void>;

export type WebSocketCloseConnectionFn = (connectionId: string, code: number, reason: string) => Promise<void> | void;

export type WsAuthContext = {
  isSubscription: boolean,
  protocol: 'ws',
  securityContext?: any,
  authInfo?: any,
  signedWithPlaygroundAuthSecret?: boolean,
  authorization?: string,
  expiresAt?: number,
};

type WsAuthenticateResult = { authContext: WsAuthContext } | { rejectMessage: unknown };

// Policy Violation
const AUTH_CLOSE_CODE = 1008;

type WebSocketCloseErrorOptions = {
  // Reported to the client the way it would be without the close
  cause?: unknown,
  // Sent to the client as is, instead of an error built from the cause
  reply?: unknown,
};

/**
 * Raised while handling a message to drop the connection's context with its subscriptions
 * and close the socket, after the client has been told why.
 */
export class WebSocketCloseError extends Error {
  // instanceof breaks when two copies of the package are loaded
  public readonly type = 'WebSocketCloseError';

  public readonly cause?: unknown;

  public readonly reply?: unknown;

  public constructor(
    public readonly code: number,
    public readonly reason: string,
    options: WebSocketCloseErrorOptions = {},
  ) {
    super(reason);

    this.cause = options.cause;
    this.reply = options.reply;
  }
}

const isWebSocketCloseError = (e: unknown): e is WebSocketCloseError => (
  typeof e === 'object' && e !== null && (e as { type?: unknown }).type === 'WebSocketCloseError'
);

export class SubscriptionServer {
  protected readonly reauthentications: Map<string, Promise<WsAuthContext | undefined>> = new Map();

  public constructor(
    protected readonly apiGateway: ApiGateway,
    protected readonly sendMessage: WebSocketSendMessageFn,
    protected readonly subscriptionStore: LocalSubscriptionStore,
    protected readonly contextAcceptor: ContextAcceptorFn,
    protected readonly closeConnection?: WebSocketCloseConnectionFn,
  ) {
  }

  protected authExpiresAt(authContext: WsAuthContext): number {
    const now = Date.now();
    const expiresAt = now + getEnv('webSocketsAuthExpireSecs') * 1000;

    // A custom checkAuth may accept a token whose exp is absent or already in the past, only the TTL applies then
    const exp = authContext.securityContext?.exp;
    if (typeof exp === 'number' && Number.isFinite(exp) && exp * 1000 > now) {
      return Math.min(expiresAt, exp * 1000);
    }

    return expiresAt;
  }

  protected isAuthContextExpired(authContext: WsAuthContext): boolean {
    return typeof authContext.expiresAt !== 'number' || Date.now() >= authContext.expiresAt;
  }

  protected async authenticate(authorization: string | undefined, isSubscription: boolean): Promise<WsAuthenticateResult> {
    const authContext: WsAuthContext = { isSubscription, protocol: 'ws' };
    await this.apiGateway.checkAuthFn(authContext, authorization);

    // TODO: ContextAcceptorFn is typed for REST request contexts, the WS auth context has no requestId
    const acceptanceResult = await this.contextAcceptor(authContext as any);
    if (!acceptanceResult.accepted) {
      return { rejectMessage: acceptanceResult.rejectMessage };
    }

    authContext.authorization = authorization;
    authContext.expiresAt = this.authExpiresAt(authContext);

    return { authContext };
  }

  protected reauthenticate(connectionId: string, expired: WsAuthContext, isSubscription: boolean): Promise<WsAuthContext | undefined> {
    const inFlight = this.reauthentications.get(connectionId);
    if (inFlight) {
      return inFlight;
    }

    const promise = (async () => {
      const result = await this.authenticate(expired.authorization, isSubscription);
      if ('rejectMessage' in result) {
        throw new CubejsHandlerError(403, 'Forbidden', 'Not authorized');
      }

      // An authorization message handled meanwhile wins over the refresh of the token it replaced
      const current = await this.subscriptionStore.getAuthContext(connectionId);
      if (current !== expired) {
        return current;
      }

      await this.subscriptionStore.setAuthContext(connectionId, result.authContext);
      return result.authContext;
    })();

    this.reauthentications.set(connectionId, promise);

    const release = () => {
      if (this.reauthentications.get(connectionId) === promise) {
        this.reauthentications.delete(connectionId);
      }
    };
    promise.then(release, release);

    return promise;
  }

  protected resultFn(connectionId: string, messageId: string | undefined, requestId: string | undefined, logNetworkUsage: boolean = true) {
    return async (message, { status } = { status: 200 }) => {
      try {
        if (logNetworkUsage) {
          this.apiGateway.log({ type: 'Outgoing network usage', service: 'api-ws', bytes: calcMessageLength(message), }, { requestId });
        }

        await this.sendMessage(connectionId, { messageId, message, status });
      } catch (e: any) {
        this.apiGateway.log({
          type: 'WebSocket Send Error',
          service: 'api-ws',
          error: e?.stack || String(e),
        }, { requestId });
      }
    };
  }

  protected replyMessageId(message: unknown): string | undefined {
    if (!message || typeof message !== 'object' || !('messageId' in message)) {
      return undefined;
    }

    return validateMessageId(message.messageId);
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

  protected validateMessage(message: unknown): WsMessage {
    if (!message || typeof message !== 'object') {
      throw new CubejsHandlerError(400, 'Invalid message format', 'Message must be a JSON object');
    }

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
    let rawMessage: unknown;

    try {
      rawMessage = this.deserializeMessage(body);
      const message = this.validateMessage(rawMessage);

      await this.handleMessage(connectionId, message, false);
    } catch (e) {
      this.apiGateway.handleError({
        e,
        res: this.resultFn(connectionId, this.replyMessageId(rawMessage), undefined, false),
      });
    }
  }

  protected async handleMessage(connectionId: string, message: WsMessage, isSubscription: boolean) {
    let authContext: WsAuthContext | undefined;
    let context: Partial<ExtendedRequestContext> = {};

    try {
      const connection = this.subscriptionStore.connect(connectionId);

      if ('authorization' in message) {
        if (connection.isAuthorizing()) {
          throw new CubejsHandlerError(409, 'Authorization in progress', 'Authorization is already in progress for this connection');
        }

        await connection.trackAuth(async () => {
          // A failed re-authentication must not leave the previous identity usable, a fresh socket has none
          const closeErrorIfAuthorized = async (options: WebSocketCloseErrorOptions) => (
            await this.subscriptionStore.getAuthContext(connectionId)
              ? new WebSocketCloseError(AUTH_CLOSE_CODE, 'Authorization failed', options)
              : undefined
          );

          let result: WsAuthenticateResult;

          try {
            result = await this.authenticate(message.authorization, isSubscription);
          } catch (e) {
            throw (await closeErrorIfAuthorized({ cause: e })) || e;
          }

          if ('rejectMessage' in result) {
            const closeError = await closeErrorIfAuthorized({ reply: result.rejectMessage });
            if (closeError) {
              throw closeError;
            }

            await this.sendMessage(connectionId, result.rejectMessage);
            return;
          }

          if (connection.isClosed()) {
            return;
          }

          await this.subscriptionStore.setAuthContext(connectionId, result.authContext);
          await this.sendMessage(connectionId, { handshake: true });
        });

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
      if (authContext && this.isAuthContextExpired(authContext)) {
        try {
          authContext = await this.reauthenticate(connectionId, authContext, isSubscription);
        } catch (e) {
          throw new WebSocketCloseError(AUTH_CLOSE_CODE, 'Authorization expired', { cause: e });
        }
      }

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

      const subscriptionId = message.messageId;
      const baseRequestId = message.requestId || `${connectionId}-${subscriptionId}`;
      // Without a requestId the id is built from the raw messageId, which the schema doesn't restrict
      if (!isValidRequestId(baseRequestId)) {
        throw new UserError(`Invalid request id: ${JSON.stringify(baseRequestId)}`);
      }

      const requestId = `${baseRequestId}-span-${uuidv4()}`;

      context = await this.apiGateway.contextByReq(
        // TODO: We need to standardize type for WS request type
        message as any,
        authContext.securityContext,
        requestId
      );

      if (connection.isClosed()) {
        return;
      }

      this.apiGateway.log({
        type: 'Incoming network usage',
        service: 'api-ws',
        bytes: calcMessageLength(message),
      }, context);

      const collectedParams: Record<string, unknown> = Object.create(null);

      if (message.params) {
        for (const k of methodParams[message.method]) {
          collectedParams[k] = message.params[k];
        }
      }

      if (collectedParams.cache !== undefined) {
        collectedParams.cacheMode = collectedParams.cache;
        delete collectedParams.cache;
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
      const closeError = isWebSocketCloseError(e) ? e : undefined;
      if (closeError) {
        await this.subscriptionStore.disconnect(connectionId);
      }

      if (closeError && closeError.reply !== undefined) {
        await this.sendMessage(connectionId, closeError.reply);
      } else {
        const messageId = 'messageId' in message ? message.messageId : undefined;
        const query = 'params' in message && message.params && 'query' in message.params
          ? message.params.query
          : undefined;

        const res = this.resultFn(connectionId, messageId, context.requestId);
        let sent: Promise<void> | undefined;

        this.apiGateway.handleError({
          e: closeError ? closeError.cause : e,
          query,
          res: (...args) => {
            sent = res(...args);
            return sent;
          },
          context
        });

        // The error has to reach the client before the socket is closed
        if (closeError) {
          await sent;
        }
      }

      if (closeError && this.closeConnection) {
        await this.closeConnection(connectionId, closeError.code, closeError.reason);
      }
    }
  }

  public async processSubscriptions() {
    const allSubscriptions = this.subscriptionStore.getAllSubscriptions();
    await Promise.all(allSubscriptions.map(async subscription => {
      // One failing subscription must not abort the tick for the others
      try {
        await this.handleMessage(subscription.connectionId, subscription.message, true);
      } catch (e: any) {
        this.apiGateway.log({
          type: 'WebSocket Subscription Error',
          service: 'api-ws',
          connectionId: subscription.connectionId,
          error: e?.stack || String(e),
        });
      }
    }));
  }

  public async disconnect(connectionId: string) {
    this.reauthentications.delete(connectionId);
    await this.subscriptionStore.disconnect(connectionId);
  }

  public clear() {
    this.subscriptionStore.clear();
  }
}
