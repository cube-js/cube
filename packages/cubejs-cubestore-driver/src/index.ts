// https://github.com/google/flatbuffers/issues/6208
// flatbuffers uses DOM... In browsers TextEncoder/Decoder (Web API) lives in global space
// https://github.com/google/flatbuffers/blob/v23.3.3/tsconfig.json

// todo: Remove after fix
if (!global.TextEncoder) {
  // eslint-disable-next-line global-require
  global.TextEncoder = require('util').TextEncoder;
}

// todo: Remove after fix
if (!global.TextDecoder) {
  // eslint-disable-next-line global-require
  global.TextDecoder = require('util').TextDecoder;
}

export * from './CubeStoreCacheDriver';
export * from './CubeStoreDriver';
export * from './CubeStoreDevDriver';
export * from './CubeStoreQueueDriver';
export * from './rexport';
