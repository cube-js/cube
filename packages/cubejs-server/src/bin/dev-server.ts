import 'source-map-support/register';

import { getEnv, markDevModeResolvedByCaller } from '@cubejs-backend/shared';

import { CubejsServer } from '../server';

// Dev mode is requested through CreateOptions rather than by writing CUBEJS_DEV_MODE,
// because that variable also gates the SQL API's port and password check. An explicit
// CUBEJS_DEV_MODE wins, matching `cubejs dev-server`
const devServer = process.env.CUBEJS_DEV_MODE === undefined || getEnv('devMode');

if (devServer) {
  markDevModeResolvedByCaller();

  // A driver cannot see CreateOptions.devServer, so DatabricksDriver resolves the
  // pre-aggregation schema from CUBEJS_DEV_MODE and would answer `prod_` while
  // server-core emits `dev_`. Pinning the schema makes both sides read the same value
  if (process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA === undefined) {
    process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA = 'dev_pre_aggregations';
  }
}

const server = new CubejsServer({ devServer });

server.listen().then(({ version, port }) => {
  console.log(`🚀 Cube server (${version}) is listening on ${port}`);
}).catch(e => {
  console.error('Fatal error during server start: ');
  console.error(e.stack || e);
});
