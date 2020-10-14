/* globals describe,test,expect,jest,afterEach */
/* eslint-disable no-underscore-dangle */

jest.mock('@cubejs-backend/server-core', () => {
  const staticCreate = jest.fn();
  const initApp = jest.fn(() => Promise.resolve());
  const event = jest.fn(() => Promise.resolve());
  const releaseConnections = jest.fn(() => Promise.resolve());
  class CubejsServerCore {
    static create() {
      // eslint-disable-next-line prefer-rest-params
      staticCreate.call(null, arguments);
      return new CubejsServerCore();
    }

    initApp() {
      return initApp();
    }

    event() {
      return event();
    }

    releaseConnections() {
      return releaseConnections();
    }
  }
  CubejsServerCore.mock = {
    staticCreate,
    initApp,
    event,
    releaseConnections,
  };
  return CubejsServerCore;
});

// eslint-disable-next-line global-require
jest.mock('http', () => require('./__mocks__/http'));
// eslint-disable-next-line global-require
jest.mock('https', () => require('./__mocks__/https'));

const http = require('http');
const https = require('https');
const CubeServer = require('./index');

describe('CubeServer', () => {
  describe('listen', () => {
    afterEach(() => jest.clearAllMocks());

    test(
      'given that CUBEJS_ENABLE_TLS is not true, ' +
      'should create an http server that listens to the PORT',
      async () => {
      // arrange
        const server = new CubeServer();
        // act
        await server.listen();
        // assert
        expect(http.createServer).toHaveBeenCalledTimes(1);
        expect(http.__mockServer.listen).toHaveBeenCalledTimes(1);
        expect(http.__mockServer.listen.mock.calls[0][0]).toBe(4000);
      }
    );

    test(
      'given that CUBEJS_ENABLE_TLS is true, ' +
      'should create an http server listening to PORT to redirect to https',
      async () => {
      // arrange
        process.env.CUBEJS_ENABLE_TLS = 'true';
        const server = new CubeServer();
        // act
        await server.listen();
        // assert
        expect(http.createServer).toHaveBeenCalledTimes(1);
        expect(http.createServer.mock.calls[0][0].toString()).toMatchSnapshot();
        expect(server.redirector).toBe(http.__mockServer);
        expect(http.__mockServer.listen).toHaveBeenCalledTimes(1);
        expect(http.__mockServer.listen.mock.calls[0][0]).toBe(4000);
      }
    );

    test(
      'given that CUBEJS_ENABLE_TLS is true, ' +
      'should create an https server that listens to the TLS_PORT',
      async () => {
      // arrange
        process.env.CUBEJS_ENABLE_TLS = 'true';
        const server = new CubeServer();
        // act
        await server.listen();
        // assert
        expect(https.createServer).toHaveBeenCalledTimes(1);
        expect(https.__mockServer.listen).toHaveBeenCalledTimes(1);
        expect(https.__mockServer.listen.mock.calls[0][0]).toBe(4433);
      }
    );

    test('given an option object, should pass the options object to the created server instance', async () => {
    // arrange
      process.env.CUBEJS_ENABLE_TLS = 'true';
      let options = { key: true, cert: true };
      let server = new CubeServer();
      // act
      await server.listen(options);
      // assert
      expect(https.createServer.mock.calls[0][0]).toBe(options);

      // arrange
      process.env.CUBEJS_ENABLE_TLS = 'false';
      options = { key: true, cert: true };
      server = new CubeServer();
      // act
      await server.listen(options);
      // assert
      expect(http.createServer.mock.calls[1][0]).toBe(options);
    });

    test('given a successful server listen, should resolve the app, the port(s) and the server instance', async () => {
    // arrange
      process.env.CUBEJS_ENABLE_TLS = 'true';
      const options = { key: true, cert: true };
      let cubeServer = new CubeServer();

      {
        // act
        const { app, port, tlsPort, server } = await cubeServer.listen(options);
        // assert
        expect(app).toBeInstanceOf(Function);
        expect(port).toBe(4000);
        expect(tlsPort).toBe(4433);
        expect(server).toBe(https.__mockServer);
      }

      // arrange
      process.env.CUBEJS_ENABLE_TLS = 'false';
      cubeServer = new CubeServer();

      {
        // act
        const { app, port, server } = await cubeServer.listen(options);
        // assert
        expect(app).toBeInstanceOf(Function);
        expect(port).toBe(4000);
        expect(server).toBe(http.__mockServer);
      }
    });

    test(
      'given a failed server listen, ' +
      'should reject the error and reset the server and redirector members to null',
      async () => {
      // arrange
        process.env.CUBEJS_ENABLE_TLS = 'false';
        const error = new Error('I\'m a Teapot');
        http.__mockServer.listen.mockImplementationOnce(
          (opts, cb) => cb && cb(error)
        );
        const cubeServer = new CubeServer();
        // act
        try {
          await cubeServer.listen();
        } catch (err) {
        // assert
          expect(err).toBe(error);
          expect(cubeServer.redirector).toBe(null);
          expect(cubeServer.server).toBe(null);
        }
      }
    );

    test('should not be able to listen if the server is already listening', async () => {
    // arrange
      process.env.CUBEJS_ENABLE_TLS = 'false';
      const cubeServer = new CubeServer();
      // act
      try {
        await cubeServer.listen();
        await cubeServer.listen();
      } catch (err) {
      // assert
        expect(err.message).toBe('CubeServer is already listening');
        expect(http.createServer).toHaveBeenCalledTimes(1);
        expect(http.__mockServer.listen).toHaveBeenCalledTimes(1);
        expect(cubeServer.server).not.toBe(null);
      }
    });
  });

  describe('close', () => {
    test('should not be able to close the server if the server isn\'t already listening', async () => {
    // arrange
      process.env.CUBEJS_ENABLE_TLS = 'false';
      let cubeServer = new CubeServer();
      // act
      try {
        await cubeServer.listen();
        await cubeServer.close();
        await cubeServer.close();
      } catch (err) {
      // assert
        expect(err.message).toBe('CubeServer is not started.');
        expect(cubeServer.server).toBe(null);
        expect(cubeServer.redirector).toBe(null);
      }

      process.env.CUBEJS_ENABLE_TLS = 'true';
      cubeServer = new CubeServer();
      // act
      try {
        await cubeServer.listen();
        await cubeServer.close();
        await cubeServer.close();
      } catch (err) {
      // assert
        expect(err.message).toBe('CubeServer is not started.');
        expect(cubeServer.server).toBe(null);
        expect(cubeServer.redirector).toBe(null);
      }
    });
  });
});
