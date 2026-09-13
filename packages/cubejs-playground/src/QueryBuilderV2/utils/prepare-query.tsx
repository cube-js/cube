import { Query, QueryOrder } from '@cubejs-client/core';

export function prepareQuery(query: Query) {
  if (Array.isArray(query.order)) {
    query.order = query.order.reduce(
      (acc, order) => {
        const [field, direction] = order;

        acc[field] = direction;

        return acc;
      },
      {} as Record<string, QueryOrder>
    );
  }
}
