import { Query, QueryOrder } from '@cubejs-client/core';

export function prepareQuery(query: Query) {
  if (Array.isArray(query.order)) {
    query.order = query.order.reduce(
      (acc, order) => {
        acc[order[0]] = order[1];

        return acc;
      },
      {} as Record<string, QueryOrder>
    );
  }
}
