import { Query } from '@cubejs-client/core';

export function getMembersList(query: Query): string[] {
  const list: string[] = [
    ...(query.dimensions || []),
    ...(query.measures || []),
    ...(query.segments || []),
  ];

  query.timeDimensions?.forEach((td) => {
    list.push(td.dimension);
  });

  if (query.order) {
    let order: [string, string][] = [];

    if (!Array.isArray(query.order)) {
      order = Object.entries(query.order);
    } else {
      order = query.order;
    }

    order.forEach(([member]) => list.push(member));
  }

  return Array.from(new Set(list));
}
