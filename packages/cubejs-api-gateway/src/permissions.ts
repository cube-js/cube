/**
 * Returns default permissions with descriptions.
 */
export const permissions = async () => ({
  liveliness: {
    rest: [
      ['/readyz', ['GET']],
      ['/livez', ['GET']],
    ],
    // graphql: { nodes: [], edges: [] },
  },
  graphql: {
    rest: [
      ['/graphql', ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']],
    ],
  },
  meta: {
    rest: [
      ['/v1/context', ['GET']],
      ['/v1/meta', ['GET']],
      ['/v1/sql', ['GET', 'POST']],
      ['/v1/dry-run', ['GET', 'POST']],
      ['/v1/pre-aggregations/can-use', ['POST']],
      ['/v1/pre-aggregations', ['GET']],
      ['/v1/pre-aggregations/security-contexts', ['GET']],
      ['/v1/pre-aggregations/timezones', ['GET']],
      ['/v1/pre-aggregations/partitions', ['POST']],
    ],
  },
  data: {
    rest: [
      ['/v1/load', ['GET', 'POST']],
      ['/v1/subscribe', ['GET']],
      ['/v1/pre-aggregations/preview', ['POST']],
    ],
  },
  jobs: {
    rest: [
      ['/v1/run-scheduled-refresh', ['GET']],
      ['/v1/pre-aggregations/jobs', ['POST']],
      ['/v1/pre-aggregations/build', ['POST']],
      ['/v1/pre-aggregations/cancel', ['POST']],
      ['/v1/pre-aggregations/queue', ['POST']],
    ],
  },
});

/**
 * Returns default permissions list (all default permissions).
 */
export const defaultPermissions = async () => ['liveliness', 'graphql', 'meta', 'data', 'jobs'];
