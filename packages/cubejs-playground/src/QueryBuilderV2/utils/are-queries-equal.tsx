import { Query } from '@cubejs-client/core';

import { getQueryHash } from './get-query-hash';

export function areQueriesEqual(query1: Query, query2: Query) {
  return getQueryHash(query1) === getQueryHash(query2);
}
