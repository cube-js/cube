export const DEFAULT_WHITE_LIST = [
  // liveliness API
  ['/readyz', ['GET']],
  ['/livez', ['GET']],

  // graphql API
  ['/graphql', ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']],

  // public API
  ['/v1/load', ['GET', 'POST']],
  ['/v1/subscribe', ['GET']],
  ['/v1/sql', ['GET', 'POST']],
  ['/v1/meta', ['GET']],
  ['/v1/run-scheduled-refresh', ['GET']],
  ['/v1/dry-run', ['GET', 'POST']],
  ['/v1/pre-aggregations/can-use', ['POST']],
  ['/v1/pre-aggregations/jobs', ['POST']],

  // system API
  ['/v1/context', ['GET']],
  ['/v1/pre-aggregations', ['GET']],
  ['/v1/pre-aggregations/security-contexts', ['GET']],
  ['/v1/pre-aggregations/timezones', ['GET']],
  ['/v1/pre-aggregations/partitions', ['POST']],
  ['/v1/pre-aggregations/preview', ['POST']],
  ['/v1/pre-aggregations/build', ['POST']],
  ['/v1/pre-aggregations/queue', ['POST']],
  ['/v1/pre-aggregations/cancel', ['POST']],
];

export const DEFAULT_BLACK_LIST = [];
