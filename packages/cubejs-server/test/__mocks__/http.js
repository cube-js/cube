/* globals jest */
/* eslint-disable no-underscore-dangle */

const http = jest.requireActual('http');

http.__mockServer = {
  on: jest.fn((signal, cb) => {
    //
  }),
  listen: jest.fn((opts, cb) => cb && cb(null)),
  close: jest.fn((cb) => cb && cb(null)),
  delete: jest.fn()
};

http.createServer = jest.fn(() => http.__mockServer);

module.exports = http;
