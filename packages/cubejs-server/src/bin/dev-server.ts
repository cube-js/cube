import 'source-map-support/register';

import { getEnv, markDevModeResolvedByCaller } from '@cubejs-backend/shared';

import { CubejsServer } from '../server';

// Dev mode is requested through CreateOptions rather than by writing CUBEJS_DEV_MODE,
// because that variable also gates the SQL API's port and password check. An explicit
// CUBEJS_DEV_MODE wins, matching `cubejs dev-server`
const devServer = process.env.CUBEJS_DEV_MODE === undefined || getEnv('devMode');

if (devServer) {
  markDevModeResolvedByCaller();
}

const server = new CubejsServer({ devServer });

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube server (${version}) is listening on ${port}`);
}).catch(e => {
  console.error('Fatal error during server start: ');
  console.error(e.stack || e);
});
