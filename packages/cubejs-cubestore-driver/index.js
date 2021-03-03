const { CubeStoreDriver } = require('./dist/src/CubeStoreDriver');
const { CubeStoreDevDriver } = require('./dist/src/CubeStoreDevDriver');
const { isCubeStoreSupported, CubeStoreHandler } = require('./dist/src/rexport');

/**
 * After 5 years working with TypeScript, now I know
 * that commonjs and nodejs require is not compatibility with using export default
 */
module.exports = CubeStoreDriver;

/**
 * It's needed to move our CLI to destructing style on import
 * Please sync this file with src/index.ts
 */
module.exports.CubeStoreDevDriver = CubeStoreDevDriver;
module.exports.isCubeStoreSupported = isCubeStoreSupported;
module.exports.CubeStoreHandler = CubeStoreHandler;
