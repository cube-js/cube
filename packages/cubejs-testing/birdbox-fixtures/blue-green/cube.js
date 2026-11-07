const CubeStoreDriver = require('@cubejs-backend/cubestore-driver');

// Each `cube_store_id` gets its own Cube Store cluster, compiled data model
// (the model reads its day parity from COMPILE_CONTEXT) and orchestrator, so the
// two clusters never share a pre-aggregation table, cache or queue.
const CUBE_STORES = {
  blue: {
    host: process.env.CUBESTORE_BLUE_HOST,
    port: process.env.CUBESTORE_BLUE_PORT,
  },
  green: {
    host: process.env.CUBESTORE_GREEN_HOST,
    port: process.env.CUBESTORE_GREEN_PORT,
  },
};

// The id comes from the JWT, so only the configured ones are accepted.
function cubeStoreId(securityContext) {
  const id = securityContext && securityContext.cube_store_id;
  if (!Object.prototype.hasOwnProperty.call(CUBE_STORES, id)) {
    throw new Error(`Unknown cube_store_id: ${id}`);
  }

  return id;
}

module.exports = {
  externalDbType: 'cubestore',

  contextToAppId: ({ securityContext }) => `CUBE_APP_${cubeStoreId(securityContext)}`,

  contextToOrchestratorId: ({ securityContext }) => `CUBE_STORE_${cubeStoreId(securityContext)}`,

  externalDriverFactory: ({ securityContext }) => {
    const { host, port } = CUBE_STORES[cubeStoreId(securityContext)];

    return new CubeStoreDriver({ host, port: parseInt(port, 10) });
  },

  // The refresh worker checks both clusters; each one rebuilds only when its
  // own refresh key moves, i.e. after the ETL of a day of its parity.
  scheduledRefreshContexts: () => Object.keys(CUBE_STORES).map(
    (id) => ({ securityContext: { cube_store_id: id } })
  ),
};
