import {
  areQueriesEqual,
  removeEmptyQueryFields,
  type Filter,
  type Query,
} from '@cubejs-client/core';

import { sanitizeFilters } from './filters';

export function cloneValue<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map((item) => cloneValue(item)) as T;
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [
        key,
        cloneValue(item),
      ])
    ) as T;
  }

  return value;
}

export function removeUndefinedDeep<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map((item) => removeUndefinedDeep(item)) as T;
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([, item]) => item !== undefined)
        .map(([key, item]) => [key, removeUndefinedDeep(item)])
    ) as T;
  }

  return value;
}

export function cleanQuery(input: Query): Query {
  const query = removeUndefinedDeep(cloneValue(input));
  const filters = sanitizeFilters((query.filters ?? []) as Filter[]);

  return removeEmptyQueryFields({
    ...query,
    filters,
  });
}

export function sanitizeValidatedQuery(input: Query): Query {
  const query = cleanQuery(input);

  return removeEmptyQueryFields({
    ...query,
    timeDimensions: (query.timeDimensions ?? []).filter(
      (value) =>
        Boolean(value.granularity || value.dateRange) ||
        'compareDateRange' in value
    ),
  });
}

export function semanticQuery(input: Query): Query {
  const semantic = { ...input };
  delete semantic.order;
  delete semantic.limit;
  delete semantic.rowLimit;
  delete semantic.offset;
  delete semantic.total;
  delete semantic.responseFormat;

  return semantic;
}

export function areSemanticQueriesEqual(left: Query, right: Query): boolean {
  return areQueriesEqual(semanticQuery(left), semanticQuery(right));
}
