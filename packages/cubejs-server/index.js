const { run } = require('@oclif/command');
const { CubejsServer } = require('./dist/src/server');
const { ServerContainer } = require('./dist/src/server/container');

/**
 * After 5 years working with TypeScript, now I know
 * that commonjs and nodejs require is not compatibility with using export default
 */
module.exports = CubejsServer;

/**
 * It's needed to move our CLI to destructing style on import
 * Please sync this file with src/index.ts
 */
module.exports.CubejsServer = CubejsServer;
module.exports.ServerContainer = ServerContainer;
module.exports.run = run;
