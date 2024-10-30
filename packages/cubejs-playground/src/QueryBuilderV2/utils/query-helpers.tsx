import { Query } from '@cubejs-client/core';

export function areQueriesRelated(query1: Query, query2: Query) {
  const arr1 = [
    ...(query1.measures || []),
    ...(query1.dimensions || []),
    ...(query1.timeDimensions || []).map((item) => JSON.stringify(item)),
  ];
  const arr2 = [
    ...(query2.measures || []),
    ...(query2.dimensions || []),
    ...(query2.timeDimensions || []).map((item) => JSON.stringify(item)),
  ];

  return arr1.some((item1) => arr2.includes(item1));
}
