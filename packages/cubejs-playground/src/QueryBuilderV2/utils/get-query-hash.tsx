import { Query } from '@cubejs-client/core';

import { validateQuery } from './validate-query';

export function getQueryHash(query: Query) {
  return JSON.stringify(validateQuery(query));
}
