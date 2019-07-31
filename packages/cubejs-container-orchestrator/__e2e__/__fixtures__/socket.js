// eslint-disable-next-line import/no-extraneous-dependencies
const io = require('socket.io-client');

function connect(url, options, ...args) {
  const RECONNECT_INTERVAL = 500;
  const CONNECTION_TIMEOUT = 5000;
  const socket = io(url, ...args);

  return new Promise((resolve, reject) => {
    socket.on('connect', () => {
      resolve(socket);
    });
    socket.on('error', () => {
      // reconnect
      socket.removeAllListeners();
      socket.close();
      setTimeout(() => socket.connect(), RECONNECT_INTERVAL);
    });
    setTimeout(() => {
      reject(new Error(`Failed to connect to ${url} for over ${CONNECTION_TIMEOUT} ms`));
    }, CONNECTION_TIMEOUT);
  });
}

module.exports = connect;
