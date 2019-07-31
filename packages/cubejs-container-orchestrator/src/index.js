const app = require('express')();
const http = require('http').createServer(app);
const io = require('socket.io')(http);

if (!process.env.CUBEJS_TEST_PORT) {
  console.error('No port specified, exiting');
  process.exit(1);
}

// //////////////////////////////

let sockets = new Map();
let nextSocketId = 0;
let exitTimer;

function createExitTimer(timeout = 10000) {
  return setTimeout(() => {
    sockets = new Map();
    http.close();
    process.exit(0);
  }, timeout);
}

app.get('/', (req, res) => {
  res.send('Hello World!');
});

io.on('connection', (socket) => {
  if (exitTimer) {
    clearTimeout(exitTimer);
  }
  const socketId = nextSocketId++;
  sockets.set(socketId, socket);
  console.log('a user connected');

  // Remove the socket when it closes
  socket.on('disconnect', () => {
    console.log('a user disconnected');
    sockets.delete(socketId);
    if (sockets.size === 0) {
      exitTimer = createExitTimer(
        process.env.CUBEJS_TEST_EXIT_TIMEOUT
      );
    }
  });
});

exitTimer = createExitTimer(10000);

http.listen(process.env.CUBEJS_TEST_PORT, () => {
  console.log(`listening on *:${process.env.CUBEJS_TEST_PORT}`);
});
