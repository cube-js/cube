"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = socketIo;
exports.getPageData = getPageData;
exports.registerPath = registerPath;
exports.unregisterPath = unregisterPath;
exports.getPageQueryData = exports.getStaticQueryData = void 0;

var _socket = _interopRequireDefault(require("socket.io-client"));

var _errorOverlayHandler = require("./error-overlay-handler");

var _normalizePagePath = _interopRequireDefault(require("./normalize-page-path"));

let socket = null;
const inFlightGetPageDataPromiseCache = {};
let staticQueryData = {};
let pageQueryData = {};

const getStaticQueryData = () => staticQueryData;

exports.getStaticQueryData = getStaticQueryData;

const getPageQueryData = () => pageQueryData;

exports.getPageQueryData = getPageQueryData;

function socketIo() {
  if (process.env.NODE_ENV !== `production`) {
    if (!socket) {
      // Try to initialize web socket if we didn't do it already
      try {
        // force websocket as transport
        socket = (0, _socket.default)({
          transports: [`websocket`]
        }); // when websocket fails, we'll try polling

        socket.on(`reconnect_attempt`, () => {
          socket.io.opts.transports = [`polling`, `websocket`];
        });

        const didDataChange = (msg, queryData) => {
          const id = msg.type === `staticQueryResult` ? msg.payload.id : (0, _normalizePagePath.default)(msg.payload.id);
          return !(id in queryData) || JSON.stringify(msg.payload.result) !== JSON.stringify(queryData[id]);
        };

        socket.on(`connect`, () => {
          // we might have disconnected so we loop over the page-data requests in flight
          // so we can get the data again
          Object.keys(inFlightGetPageDataPromiseCache).forEach(pathname => {
            socket.emit(`getDataForPath`, pathname);
          });
        });
        socket.on(`message`, msg => {
          if (msg.type === `staticQueryResult`) {
            if (didDataChange(msg, staticQueryData)) {
              staticQueryData = { ...staticQueryData,
                [msg.payload.id]: msg.payload.result
              };
            }
          } else if (msg.type === `pageQueryResult`) {
            if (didDataChange(msg, pageQueryData)) {
              pageQueryData = { ...pageQueryData,
                [(0, _normalizePagePath.default)(msg.payload.id)]: msg.payload.result
              };
            }
          } else if (msg.type === `overlayError`) {
            if (msg.payload.message) {
              (0, _errorOverlayHandler.reportError)(msg.payload.id, msg.payload.message);
            } else {
              (0, _errorOverlayHandler.clearError)(msg.payload.id);
            }
          }

          if (msg.type && msg.payload) {
            ___emitter.emit(msg.type, msg.payload);
          }
        }); // Prevents certain browsers spamming XHR 'ERR_CONNECTION_REFUSED'
        // errors within the console, such as when exiting the develop process.

        socket.on(`disconnect`, () => {
          console.warn(`[socket.io] Disconnected from dev server.`);
        });
      } catch (err) {
        console.error(`Could not connect to socket.io on dev server.`);
      }
    }

    return socket;
  } else {
    return null;
  }
}

function getPageData(pathname) {
  pathname = (0, _normalizePagePath.default)(pathname);

  if (inFlightGetPageDataPromiseCache[pathname]) {
    return inFlightGetPageDataPromiseCache[pathname];
  } else {
    inFlightGetPageDataPromiseCache[pathname] = new Promise(resolve => {
      if (pageQueryData[pathname]) {
        delete inFlightGetPageDataPromiseCache[pathname];
        resolve(pageQueryData[pathname]);
      } else {
        const onPageDataCallback = msg => {
          if (msg.type === `pageQueryResult` && (0, _normalizePagePath.default)(msg.payload.id) === pathname) {
            socket.off(`message`, onPageDataCallback);
            delete inFlightGetPageDataPromiseCache[pathname];
            resolve(pageQueryData[pathname]);
          }
        };

        socket.on(`message`, onPageDataCallback);
        socket.emit(`getDataForPath`, pathname);
      }
    });
  }

  return inFlightGetPageDataPromiseCache[pathname];
} // Tell websocket-manager.js the new path we're on.
// This will help the backend prioritize queries for this
// path.


function registerPath(path) {
  socket.emit(`registerPath`, path);
} // Unregister the former path


function unregisterPath(path) {
  socket.emit(`unregisterPath`, path);
}