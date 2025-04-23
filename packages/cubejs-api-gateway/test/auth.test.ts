// eslint-disable-next-line import/no-extraneous-dependencies
import express, { Application as ExpressApplication, RequestHandler } from 'express';
// eslint-disable-next-line import/no-extraneous-dependencies
import request from 'supertest';
import jwt from 'jsonwebtoken';
import { pausePromise } from '@cubejs-backend/shared';
import { resetLogger } from '@cubejs-backend/native';

import { ApiGateway, ApiGatewayOptions, CubejsHandlerError, Request, RequestContext } from '../src';
import { AdapterApiMock, DataSourceStorageMock } from './mocks';
import { generateAuthToken } from './utils';

class ApiGatewayOpenAPI extends ApiGateway {
  protected isRunning: Promise<void> | null = null;

  public coerceForSqlQuery(query, context: RequestContext) {
    return super.coerceForSqlQuery(query, context);
  }

  public async startSQLServer(): Promise<void> {
    if (this.isRunning) {
      return this.isRunning;
    }

    this.isRunning = this.sqlServer.init({});

    return this.isRunning;
  }

  public async shutdownSQLServer(): Promise<void> {
    try {
      await this.sqlServer.shutdown('fast');
    } finally {
      this.isRunning = null;
    }

    // SQLServer changes logger for rust side with setupLogger in the constructor, but it leads
    // to a memory leak, that's why jest doesn't allow to shut down tests
    resetLogger(
      process.env.CUBEJS_LOG_LEVEL === 'trace' ? 'trace' : 'warn'
    );
  }
}

function createApiGateway(handler: RequestHandler, logger: () => any, options: Partial<ApiGatewayOptions>) {
  const adapterApi: any = new AdapterApiMock();
  const dataSourceStorage: any = new DataSourceStorageMock();

  class ApiGatewayFake extends ApiGatewayOpenAPI {
    public initApp(app: ExpressApplication) {
      const userMiddlewares: RequestHandler[] = [
        this.checkAuth,
        this.requestContextMiddleware,
      ];

      app.get('/test-auth-fake', userMiddlewares, handler);
      this.enableNativeApiGateway(app);

      app.use(this.handleErrorMiddleware);
    }
  }

  const apiGateway = new ApiGatewayFake('secret', <any>null, () => adapterApi, logger, {
    standalone: true,
    dataSourceStorage,
    basePath: '/cubejs-api',
    refreshScheduler: {},
    enforceSecurityChecks: true,
    ...options,
  });

  process.env.NODE_ENV = 'unknown';

  const app = express();
  apiGateway.initApp(app);

  return {
    apiGateway,
    app,
  };
}

describe('test authorization with native gateway', () => {
  let app: ExpressApplication;
  let apiGateway: ApiGatewayOpenAPI;

  const handlerMock = jest.fn(() => {
    // nothing, we are using it to verify that we don't got to express code
  });
  const loggerMock = jest.fn(() => {
    //
  });
  const checkAuthMock = jest.fn((req, token) => {
    jwt.verify(token, 'secret');

    return {
      security_context: {}
    };
  });

  beforeAll(async () => {
    const result = createApiGateway(handlerMock, loggerMock, {
      checkAuth: checkAuthMock,
      gatewayPort: 8585,
    });

    app = result.app;
    apiGateway = result.apiGateway;

    await result.apiGateway.startSQLServer();
  });

  beforeEach(() => {
    handlerMock.mockClear();
    loggerMock.mockClear();
    checkAuthMock.mockClear();
  });

  afterAll(async () => {
    await apiGateway.shutdownSQLServer();
  });

  it('default authorization - success', async () => {
    const token = generateAuthToken({ uid: 5, });

    await request(app)
      .get('/cubejs-api/v2/stream')
      .set('Authorization', `${token}`)
      .send()
      .expect(501);

    // No bad logs
    expect(loggerMock.mock.calls.length).toEqual(0);
    // We should not call js handler, request should go into rust code
    expect(handlerMock.mock.calls.length).toEqual(0);

    // Verify that we passed token to JS side
    expect(checkAuthMock.mock.calls.length).toEqual(1);
    expect(checkAuthMock.mock.calls[0][0].protocol).toEqual('http');
    expect(checkAuthMock.mock.calls[0][1]).toEqual(token);
  });

  it('default authorization - success (bearer prefix)', async () => {
    const token = generateAuthToken({ uid: 5, });

    await request(app)
      .get('/cubejs-api/v2/stream')
      .set('Authorization', `Bearer ${token}`)
      .send()
      .expect(501);

    // No bad logs
    expect(loggerMock.mock.calls.length).toEqual(0);
    // We should not call js handler, request should go into rust code
    expect(handlerMock.mock.calls.length).toEqual(0);

    // Verify that we passed token to JS side
    expect(checkAuthMock.mock.calls.length).toEqual(1);
    expect(checkAuthMock.mock.calls[0][0].protocol).toEqual('http');
    expect(checkAuthMock.mock.calls[0][1]).toEqual(token);
  });

  it('default authorization - wrong secret', async () => {
    const badToken = 'SUPER_LARGE_BAD_TOKEN_WHICH_IS_NOT_A_TOKEN';

    await request(app)
      .get('/cubejs-api/v2/stream')
      .set('Authorization', `${badToken}`)
      .send()
      .expect(401);

    // No bad logs
    expect(loggerMock.mock.calls.length).toEqual(0);
    // We should not call js handler, request should go into rust code
    expect(handlerMock.mock.calls.length).toEqual(0);

    // Verify that we passed token to JS side
    expect(checkAuthMock.mock.calls.length).toEqual(1);
    expect(checkAuthMock.mock.calls[0][0].protocol).toEqual('http');
    expect(checkAuthMock.mock.calls[0][1]).toEqual(badToken);
  });

  it('default authorization - missing auth header', async () => {
    await request(app)
      .get('/cubejs-api/v2/stream')
      .send()
      .expect(401);

    // No bad logs
    expect(loggerMock.mock.calls.length).toEqual(0);
    // We should not call js handler, request should go into rust code
    expect(handlerMock.mock.calls.length).toEqual(0);
  });
});

describe('test authorization', () => {
  test('default authorization', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const expectSecurityContext = (securityContext) => {
      expect(securityContext.uid).toEqual(5);
      expect(securityContext.iat).toBeDefined();
      expect(securityContext.exp).toBeDefined();
    };

    const handlerMock = jest.fn((req, res) => {
      expectSecurityContext(req.context.authInfo);
      expectSecurityContext(req.context.securityContext);

      res.status(200).end();
    });

    const { app } = createApiGateway(handlerMock, loggerMock, {});

    const token = generateAuthToken({ uid: 5, });

    await request(app)
      .get('/test-auth-fake')
      .set('Authorization', `Authorization: ${token}`)
      .expect(200);

    // No bad logs
    expect(loggerMock.mock.calls.length).toEqual(0);
    expect(handlerMock.mock.calls.length).toEqual(1);

    expectSecurityContext(handlerMock.mock.calls[0][0].context.securityContext);
    // authInfo was deprecated, but should exists as computability
    expectSecurityContext(handlerMock.mock.calls[0][0].context.authInfo);
  });

  test('playground auth token', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const expectSecurityContext = (securityContext) => {
      expect(securityContext.uid).toEqual(5);
      expect(securityContext.iat).toBeDefined();
      expect(securityContext.exp).toBeDefined();
    };

    const handlerMock = jest.fn((req, res) => {
      expectSecurityContext(req.context.authInfo);
      expectSecurityContext(req.context.securityContext);

      res.status(200).end();
    });

    const playgroundAuthSecret = 'playgroundSecret';
    const { app } = createApiGateway(handlerMock, loggerMock, {
      playgroundAuthSecret
    });

    const token = generateAuthToken({ uid: 5, }, {});
    const playgroundToken = generateAuthToken({ uid: 5, }, {}, playgroundAuthSecret);
    const badToken = generateAuthToken({ uid: 5, }, {}, 'bad');

    await request(app)
      .get('/test-auth-fake')
      .set('Authorization', `Authorization: ${token}`)
      .expect(200);

    await request(app)
      .get('/test-auth-fake')
      .set('Authorization', `Authorization: ${playgroundToken}`)
      .expect(200);

    await request(app)
      .get('/test-auth-fake')
      .set('Authorization', `Authorization: ${badToken}`)
      .expect(403);

    expect(loggerMock.mock.calls.length).toEqual(1);
    expect(handlerMock.mock.calls.length).toEqual(2);

    expectSecurityContext(handlerMock.mock.calls[0][0].context.securityContext);
    // authInfo was deprecated, but should exists as computability
    expectSecurityContext(handlerMock.mock.calls[0][0].context.authInfo);
  });

  test('default authorization with JWT token and securityContext in u', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const expectSecurityContext = (securityContext) => {
      expect(securityContext.u).toEqual({
        uid: 5,
      });
      expect(securityContext.iat).toBeDefined();
      expect(securityContext.exp).toBeDefined();
    };

    const handlerMock = jest.fn((req, res) => {
      expectSecurityContext(req.context.securityContext);
      expectSecurityContext(req.context.authInfo);

      res.status(200).end();
    });

    const { app } = createApiGateway(handlerMock, loggerMock, {});

    const token = generateAuthToken({ u: { uid: 5, } });

    await request(app)
      .get('/test-auth-fake')
      .set('Authorization', `Authorization: ${token}`)
      .expect(200);

    expect(loggerMock.mock.calls.length).toEqual(0);
    expect(handlerMock.mock.calls.length).toEqual(1);
  });

  test('custom checkAuth with async flow', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const expectSecurityContext = (securityContext) => {
      expect(securityContext.uid).toEqual(5);
      expect(securityContext.iat).toBeDefined();
      expect(securityContext.exp).toBeDefined();
    };

    const handlerMock = jest.fn((req, res) => {
      expectSecurityContext(req.context.securityContext);
      expectSecurityContext(req.context.authInfo);

      res.status(200).end();
    });

    const { app } = createApiGateway(handlerMock, loggerMock, {
      checkAuth: async (req: Request, auth?: string) => {
        if (auth) {
          await pausePromise(500);

          req.authInfo = jwt.verify(auth, 'secret');
        }
      }
    });

    const token = generateAuthToken({ uid: 5, });

    await request(app)
      .get('/test-auth-fake')
      .set('Authorization', `Authorization: ${token}`)
      .expect(200);

    expect(loggerMock.mock.calls.length).toEqual(1);
    expect(loggerMock.mock.calls[0]).toEqual([
      'AuthInfo Deprecation',
      {
        warning: 'authInfo was renamed to securityContext, please migrate: https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#checkauthmiddleware',
      }
    ]);

    expect(handlerMock.mock.calls.length).toEqual(1);

    expectSecurityContext(handlerMock.mock.calls[0][0].context.securityContext);
    // authInfo was deprecated, but should exists as computability
    expectSecurityContext(handlerMock.mock.calls[0][0].context.authInfo);
  });

  test('custom checkAuth with async flow and throw exception', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const handlerMock = jest.fn((req, res) => {
      res.status(200).end();
    });

    const { app } = createApiGateway(handlerMock, loggerMock, {
      checkAuth: async () => {
        throw new CubejsHandlerError(555, 'unknown', 'unknown message');
      }
    });

    const token = generateAuthToken({ uid: 5, });

    const res = await request(app)
      .get('/test-auth-fake')
      .set('Authorization', `Authorization: ${token}`)
      .expect(555);

    expect(res.body).toMatchObject({
      error: 'unknown message'
    });
  });

  test('custom checkAuth with async flow and return', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const expectSecurityContext = (securityContext) => {
      expect(securityContext.uid).toEqual(5);
      expect(securityContext.iat).toBeDefined();
      expect(securityContext.exp).toBeDefined();
    };

    const handlerMock = jest.fn((req, res) => {
      expectSecurityContext(req.context.securityContext);
      expectSecurityContext(req.context.authInfo);

      res.status(200).end();
    });

    const { app } = createApiGateway(handlerMock, loggerMock, {
      checkAuth: async (req: Request, auth?: string) => {
        if (auth) {
          await pausePromise(500);

          const securityContext = jwt.verify(auth, 'secret');

          req.securityContext = {
            uid: 'should not be visible',
          };

          return {
            security_context: securityContext,
          };
        }

        return {};
      }
    });

    const token = generateAuthToken({ uid: 5, });

    await request(app)
      .get('/test-auth-fake')
      .set('Authorization', `Authorization: ${token}`)
      .expect(200);

    expect(handlerMock.mock.calls.length).toEqual(1);

    expectSecurityContext(handlerMock.mock.calls[0][0].context.securityContext);
    // authInfo was deprecated, but should exist as computability
    expectSecurityContext(handlerMock.mock.calls[0][0].context.authInfo);
  });

  test('custom checkAuth with CubejsHandlerError fail in playground', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const expectSecurityContext = (securityContext) => {
      expect(securityContext.uid).toEqual(5);
      expect(securityContext.iat).toBeDefined();
      expect(securityContext.exp).toBeDefined();
    };

    const handlerMock = jest.fn((req, res) => {
      expectSecurityContext(req.context.securityContext);
      expectSecurityContext(req.context.authInfo);

      res.status(200).end();
    });

    const playgroundAuthSecret = 'playgroundSecret';

    const token = generateAuthToken({ uid: 5, }, {});

    const { app } = createApiGateway(handlerMock, loggerMock, {
      playgroundAuthSecret,
      checkAuth: async (_req: Request, _auth?: string) => {
        throw new CubejsHandlerError(409, 'Error', 'Custom error');
      }
    });

    const res = await request(app)
      .get('/test-auth-fake')
      .set('Authorization', `Authorization: ${token}`)
      .expect(409);

    expect(res.body).toMatchObject({
      error: 'Custom error'
    });
  });

  test('custom checkAuth with deprecated authInfo', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const EXPECTED_SECURITY_CONTEXT = {
      exp: 2475857705, iat: 1611857705, uid: 5
    };

    const handlerMock = jest.fn((req, res) => {
      expect(req.context.securityContext).toEqual(EXPECTED_SECURITY_CONTEXT);
      expect(req.context.authInfo).toEqual(EXPECTED_SECURITY_CONTEXT);

      res.status(200).end();
    });

    const { app } = createApiGateway(handlerMock, loggerMock, {
      checkAuth: (req: Request, auth?: string) => {
        if (auth) {
          req.authInfo = jwt.verify(auth, 'secret');
        }
      }
    });

    await request(app)
      .get('/test-auth-fake')
      // console.log(generateAuthToken({ uid: 5, }));
      .set('Authorization', 'Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g')
      .expect(200);

    expect(loggerMock.mock.calls.length).toEqual(1);
    expect(loggerMock.mock.calls[0]).toEqual([
      'AuthInfo Deprecation',
      {
        warning: 'authInfo was renamed to securityContext, please migrate: https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#checkauthmiddleware',
      }
    ]);

    expect(handlerMock.mock.calls.length).toEqual(1);

    expect(handlerMock.mock.calls[0][0].context.securityContext).toEqual(EXPECTED_SECURITY_CONTEXT);
    // authInfo was deprecated, but should exists as computability
    expect(handlerMock.mock.calls[0][0].context.authInfo).toEqual(EXPECTED_SECURITY_CONTEXT);
  });

  test('custom checkAuth with securityContext (not object)', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const EXPECTED_SECURITY_CONTEXT = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g';

    const handlerMock = jest.fn((req, res) => {
      expect(req.context.securityContext).toEqual(EXPECTED_SECURITY_CONTEXT);
      expect(req.context.authInfo).toEqual(EXPECTED_SECURITY_CONTEXT);

      res.status(200).end();
    });

    const { app } = createApiGateway(handlerMock, loggerMock, {
      checkAuth: (req: Request, auth?: string) => {
        if (auth) {
          // It must be object, but some users are using string for securityContext
          req.securityContext = auth;
        }
      }
    });

    await request(app)
      .get('/test-auth-fake')
      // console.log(generateAuthToken({ uid: 5, }));
      .set('Authorization', 'Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g')
      .expect(200);

    expect(loggerMock.mock.calls.length).toEqual(1);
    expect(loggerMock.mock.calls[0]).toEqual([
      'Security Context Should Be Object',
      {
        warning: 'Value of securityContext (previously authInfo) expected to be object, actual: string',
      }
    ]);

    expect(handlerMock.mock.calls.length).toEqual(1);

    expect(handlerMock.mock.calls[0][0].context.securityContext).toEqual(EXPECTED_SECURITY_CONTEXT);
    // authInfo was deprecated, but should exists as computability
    expect(handlerMock.mock.calls[0][0].context.authInfo).toEqual(EXPECTED_SECURITY_CONTEXT);
  });

  test('coerceForSqlQuery multiple', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const handlerMock = jest.fn();

    const { apiGateway } = createApiGateway(handlerMock, loggerMock, {});

    // handle null
    expect(
      apiGateway.coerceForSqlQuery(
        { timeDimensions: [] },
        { securityContext: null, requestId: 'XXX' }
      ).contextSymbols.securityContext
    ).toEqual({});
    // no warnings, done on checkAuth/checkAuthMiddleware level
    expect(loggerMock.mock.calls.length).toEqual(0);

    // handle string
    expect(
      apiGateway.coerceForSqlQuery(
        { timeDimensions: [] },
        { securityContext: 'AAABBBCCC', requestId: 'XXX' }
      ).contextSymbols.securityContext
    ).toEqual({});
    // no warnings, done on checkAuth/checkAuthMiddleware level
    expect(loggerMock.mock.calls.length).toEqual(0);

    /**
     * Original securityContext should not be changed by coerceForSqlQuery, because SubscriptionServer store it once
     * for all queries
     */
    const securityContext = { exp: 2475858836, iat: 1611858836, u: { uid: 5 } };

    // (move u to root)
    expect(
      apiGateway.coerceForSqlQuery(
        { timeDimensions: [] },
        { securityContext, requestId: 'XXX' }
      ).contextSymbols.securityContext
    ).toEqual({
      exp: 2475858836,
      iat: 1611858836,
      uid: 5,
    });

    // (move u to root)
    expect(
      apiGateway.coerceForSqlQuery(
        { timeDimensions: [] },
        { securityContext, requestId: 'XXX' }
      ).contextSymbols.securityContext
    ).toEqual({
      exp: 2475858836,
      iat: 1611858836,
      uid: 5,
    });

    expect(securityContext).toEqual({ exp: 2475858836, iat: 1611858836, u: { uid: 5 } });

    expect(loggerMock.mock.calls.length).toEqual(1);
    expect(loggerMock.mock.calls[0]).toEqual([
      'JWT U Property Deprecation',
      {
        warning: 'Storing security context in the u property within the payload is now deprecated, please migrate: https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#authinfo',
      }
    ]);
  });

  test('coerceForSqlQuery claimsNamespace', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const handlerMock = jest.fn();

    const { apiGateway } = createApiGateway(handlerMock, loggerMock, {
      jwt: {
        claimsNamespace: 'http://localhost:4000'
      }
    });

    // handle null
    expect(
      apiGateway.coerceForSqlQuery(
        { timeDimensions: [] },
        { securityContext: {}, requestId: 'XXX' }
      ).contextSymbols.securityContext
    ).toEqual({});
    // no warnings, done on checkAuth/checkAuthMiddleware level
    expect(loggerMock.mock.calls.length).toEqual(0);

    // handle ok
    expect(
      apiGateway.coerceForSqlQuery(
        { timeDimensions: [] },
        { securityContext: { 'http://localhost:4000': { uid: 5 } }, requestId: 'XXX' }
      ).contextSymbols.securityContext
    ).toEqual({ uid: 5 });
    // no warnings, done on checkAuth/checkAuthMiddleware level
    expect(loggerMock.mock.calls.length).toEqual(0);
  });
});
