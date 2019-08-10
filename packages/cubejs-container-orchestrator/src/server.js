const express = require('express');
const http = require('http');
const util = require('util');
const socketIO = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = socketIO(server);
var sockets;
var nextSocketId;
var shutdownTimer;

module.exports = {
  start,
  shutdown,
  createShutdownTimer,
  get app() {
    return app || null;
  },
  get io() {
    return io || null;
  },
  get sockets() {
    return sockets || null;
  },
};

// quit on ctrl-c when running docker in terminal
process.on("SIGINT", () => {
  console.info(
    "Got SIGINT (aka ctrl-c in docker). Graceful shutdown ",
    new Date().toISOString()
  );
  shutdown();
});

// quit properly on docker stop
process.on("SIGTERM", () => {
  console.info(
    "Got SIGTERM (docker container stop). Graceful shutdown ",
    new Date().toISOString()
  );
  shutdown();
});

if (!process.env.CUBEJS_TEST_PORT) {
  console.error("No port specified, exiting");
  process.exit(1);
}

// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////

async function start() {
  sockets = new Map();
  nextSocketId = 0;

  io.on("connection", socket => {
    if (shutdownTimer) {
      clearTimeout(shutdownTimer);
    }
    const socketId = nextSocketId++;
    sockets.set(socketId, socket);

    // Remove the socket when it closes
    socket.on("disconnect", () => {
      sockets.delete(socketId);
      if (sockets.size === 0) {
        shutdownTimer = createShutdownTimer(process.env.CUBEJS_TEST_EXIT_TIMEOUT);
      }
    });
  });

  server.listen(process.env.CUBEJS_TEST_PORT, () => {
    console.log(`listening on *:${process.env.CUBEJS_TEST_PORT}`);
  });
  server.close = util.promisify(server.close);
}

async function shutdown() {
  try {
    if (server) {
      await server.close();
    }
    sockets = new Map();
  } catch (err) {
    console.error(err);
    process.exitCode = 1;
  } finally {
    process.exit();
  }
}

function createShutdownTimer(timeout = process.env.CUBEJS_TEST_EXIT_TIMEOUT || 10000) {
  if (shutdownTimer) {
    return shutdownTimer;
  }
  shutdownTimer = setTimeout(shutdown, timeout);
  return shutdownTimer;
}
