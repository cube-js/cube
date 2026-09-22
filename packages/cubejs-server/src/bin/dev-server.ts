import 'source-map-support/register';

import { markDevModeResolvedByCaller } from '@cubejs-backend/shared';

import { CubejsServer } from '../server';

// This bin is the dev server, so it asks for dev mode through CreateOptions.devServer
// rather than by writing CUBEJS_DEV_MODE: that variable also gates the SQL API's port
// and its password check, which this command never turned on. An explicit
// CUBEJS_DEV_MODE wins over it, matching `cubejs dev-server`
const devServer = process.env.CUBEJS_DEV_MODE === undefined ? true : undefined;

if (devServer) {
  // The deprecation warning has nothing to tell a process that just resolved dev mode
  // for itself
  markDevModeResolvedByCaller();
}

const server = new CubejsServer(devServer ? { devServer } : {});

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube server (${version}) is listening on ${port}`);
}).catch(e => {
  console.error('Fatal error during server start: ');
  console.error(e.stack || e);
});
