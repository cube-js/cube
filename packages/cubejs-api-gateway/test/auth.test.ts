import express, { Application as ExpressApplication, RequestHandler } from 'express';
import request from 'supertest';
import jwt from 'jsonwebtoken';

import { ApiGateway, ApiGatewayOptions, Request } from '../src';
import { AdapterApiMock, DataSourceStorageMock } from './index.test';
import { RequestContext } from '../src/interfaces';

function createApiGateway(handler: RequestHandler, logger: () => any, options: Partial<ApiGatewayOptions>) {
  const adapterApi: any = new AdapterApiMock();
  const dataSourceStorage: any = new DataSourceStorageMock();

  class ApiGatewayFake extends ApiGateway {
    public coerceForSqlQuery(query, context: RequestContext) {
      return super.coerceForSqlQuery(query, context);
    }

    public initApp(app: ExpressApplication) {
      const userMiddlewares: RequestHandler[] = [
        this.checkAuthMiddleware,
        this.requestContextMiddleware,
      ];

      app.get('/test-auth-fake', userMiddlewares, handler);
    }
  }

  const apiGateway = new ApiGatewayFake('secret', <any>null, () => adapterApi, logger, {
    standalone: true,
    dataSourceStorage,
    basePath: '/cubejs-api',
    refreshScheduler: {},
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

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function generateAuthToken(payload: object = {}) {
  return jwt.sign(payload, 'secret', {
    expiresIn: '10000d'
  });
}

describe('test authorization', () => {
  test('default authorization', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const handlerMock = jest.fn((req, res) => {
      res.status(200).end();
    });

    const { app } = createApiGateway(handlerMock, loggerMock, {});

    await request(app)
      .get('/test-auth-fake')
      // console.log(generateAuthToken({ uid: 5, }));
      .set('Authorization', 'Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g')
      .expect(200);

    // No bad logs
    expect(loggerMock.mock.calls.length).toEqual(0);
    expect(handlerMock.mock.calls.length).toEqual(1);

    const securityContext = {
      exp: 2475857705, iat: 1611857705, uid: 5
    };

    expect(handlerMock.mock.calls[0][0].context.securityContext).toEqual(securityContext);
    // authInfo was deprecated, but should exists as compatbility
    expect(handlerMock.mock.calls[0][0].context.authInfo).toEqual(securityContext);
  });

  test('default authorization with JWT token and securityContext in u', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const handlerMock = jest.fn((req, res) => {
      res.status(200).end();
    });

    const { apiGateway, app } = createApiGateway(handlerMock, loggerMock, {});

    await request(app)
      .get('/test-auth-fake')
      // console.log(generateAuthToken({ u: { uid: 5, } }));
      .set('Authorization', 'Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1Ijp7InVpZCI6NX0sImlhdCI6MTYxMTg1ODgzNiwiZXhwIjoyNDc1ODU4ODM2fQ.mxHxxzrvEmzKu86NoXOpbpxKPc5rxdbK0Qfxvnvj4B0')
      .expect(200);

    expect(loggerMock.mock.calls.length).toEqual(0);
    expect(handlerMock.mock.calls.length).toEqual(1);

    const securityContext = {
      exp: 2475858836, iat: 1611858836, uid: 5
    };

    const args: any = handlerMock.mock.calls[0];

    expect(apiGateway.coerceForSqlQuery({ timeDimensions: [] }, args[0]).contextSymbols.securityContext).toEqual(
      securityContext
    );
  });

  test('custom checkAuth with deprecated authInfo', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const handlerMock = jest.fn((req, res) => {
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

    const securityContext = {
      exp: 2475857705, iat: 1611857705, uid: 5
    };

    expect(handlerMock.mock.calls[0][0].context.securityContext).toEqual(securityContext);
    // authInfo was deprecated, but should exists as compatbility
    expect(handlerMock.mock.calls[0][0].context.authInfo).toEqual(securityContext);
  });

  test('custom checkAuthMiddleware with deprecated authInfo', async () => {
    const loggerMock = jest.fn(() => {
      //
    });

    const handlerMock = jest.fn((req, res) => {
      res.status(200).end();
    });

    const { app } = createApiGateway(handlerMock, loggerMock, {
      checkAuthMiddleware: (req: Request, res, next) => {
        if (req.headers.authorization) {
          req.authInfo = jwt.verify(req.headers.authorization, 'secret');
        }

        if (next) {
          next();
        }
      }
    });

    await request(app)
      .get('/test-auth-fake')
      // console.log(generateAuthToken({ uid: 5, }));
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g')
      .expect(200);

    expect(loggerMock.mock.calls.length).toEqual(1);
    expect(loggerMock.mock.calls[0]).toEqual([
      'CheckAuthMiddleware Middleware Deprecation',
      {
        warning: 'Option checkAuthMiddleware is now deprecated in favor of checkAuth, please migrate: https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#checkauthmiddleware',
      }
    ]);

    expect(handlerMock.mock.calls.length).toEqual(1);

    const securityContext = {
      exp: 2475857705, iat: 1611857705, uid: 5
    };

    expect(handlerMock.mock.calls[0][0].context.securityContext).toEqual(securityContext);
    // authInfo was deprecated, but should exists as compatbility
    expect(handlerMock.mock.calls[0][0].context.authInfo).toEqual(securityContext);
  });
});
