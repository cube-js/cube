/* globals jest */
/* eslint-disable no-underscore-dangle */

const staticCreate = jest.fn();
const beforeShutdown = jest.fn(() => Promise.resolve());
const shutdown = jest.fn(() => Promise.resolve());
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

  beforeShutdown() {
    return beforeShutdown();
  }

  shutdown() {
    return shutdown();
  }

  event() {
    return event();
  }

  releaseConnections() {
    return releaseConnections();
  }
}
CubejsServerCore.__mockServer = {
  staticCreate,
  initApp,
  beforeShutdown,
  shutdown,
  event,
  releaseConnections,
};

module.exports = CubejsServerCore;
