const path = require('path');
const { FileRepository } = require('@cubejs-backend/server-core');

// Several tenants served by one Cube process: each tenant gets its own
// compiler, orchestrator and pre-aggregation schema, selected by the
// `tenant` claim of the security context.
const TENANTS = ['tenant_a', 'tenant_b'];

function tenantOf(securityContext) {
  const tenant = securityContext?.tenant;
  if (!TENANTS.includes(tenant)) {
    throw new Error(`Unknown tenant: ${tenant}`);
  }

  return tenant;
}

module.exports = {
  contextToAppId: ({ securityContext }) => `CUBE_APP_${tenantOf(securityContext)}`,
  contextToOrchestratorId: ({ securityContext }) => `CUBE_ORCH_${tenantOf(securityContext)}`,
  preAggregationsSchema: ({ securityContext }) => `pre_aggregations_${tenantOf(securityContext)}`,
  repositoryFactory: ({ securityContext }) => new FileRepository(
    path.join('schema', tenantOf(securityContext))
  ),
  scheduledRefreshContexts: () => TENANTS.map((tenant) => ({ securityContext: { tenant } })),
};
