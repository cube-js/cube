import { Filter, Query } from '@cubejs-client/core';

export function getQueryHash(query: Query) {
  const queryCopy = JSON.parse(JSON.stringify(query));

  if (queryCopy.measures?.length === 0) {
    delete queryCopy.measures;
  }

  if (queryCopy.dimensions?.length === 0) {
    delete queryCopy.dimensions;
  }

  if (queryCopy.timeDimensions?.length === 0) {
    delete queryCopy.timeDimensions;
  }

  if (queryCopy.segments?.length === 0) {
    delete queryCopy.segments;
  }

  if (['{}', '[]'].includes(JSON.stringify(queryCopy.order))) {
    delete queryCopy.order;
  }

  if (JSON.stringify(queryCopy.filters) === '[]') {
    delete queryCopy.filters;
  }

  if (queryCopy.filters) {
    queryCopy.filters = queryCopy.filters.sort((a: Filter, b: Filter) =>
      'member' in a && 'member' in b ? a?.member?.localeCompare(b?.member || '') ?? 0 : 0
    );
  }

  const orderedQuery = Object.keys(queryCopy)
    .sort()
    .reduce((acc, key) => {
      acc[key as keyof Query] = queryCopy[key];

      return acc;
    }, {} as Query);

  return JSON.stringify(orderedQuery);
}
