const http = jest.requireActual("http");

http.__mockServer = {
  listen: jest.fn((opts, cb) => cb && cb(null)),
  close: jest.fn((cb) => cb && cb(null)),
  delete: jest.fn()
};

http.createServer = jest.fn(() => http.__mockServer);


module.exports = http;
