import type {
  BinaryFilter,
  Filter,
  UnaryFilter,
  UnaryOperator,
} from '@cubejs-client/core';

import type { FilterPath } from '../types';

const UNARY_OPERATORS = new Set<UnaryOperator>(['set', 'notSet']);

export function sanitizeFilters(filters: readonly Filter[] = []): Filter[] {
  const sanitized: Filter[] = [];

  for (const filter of filters) {
    if ('and' in filter) {
      const children = sanitizeFilters(filter.and);
      if (children.length) {
        sanitized.push({ and: children });
      }
      continue;
    }

    if ('or' in filter) {
      const children = sanitizeFilters(filter.or);
      if (children.length) {
        sanitized.push({ or: children });
      }
      continue;
    }

    const member = filter.member ?? filter.dimension;
    if (!member || !filter.operator) {
      continue;
    }

    if (UNARY_OPERATORS.has(filter.operator as UnaryOperator)) {
      sanitized.push({
        member,
        operator: filter.operator,
      } as UnaryFilter);
      continue;
    }

    sanitized.push({
      member,
      operator: filter.operator,
      values: [...(filter.values ?? [])],
    } as BinaryFilter);
  }

  return sanitized;
}

export function flattenFilterTree(
  filters: readonly Filter[] = [],
  parentPath: FilterPath = []
): Array<{ filter: BinaryFilter | UnaryFilter; path: FilterPath }> {
  return filters.flatMap((filter, index) => {
    const path: FilterPath = [...parentPath, index];

    if ('and' in filter) {
      return flattenFilterTree(filter.and, [...path, 'and']);
    }

    if ('or' in filter) {
      return flattenFilterTree(filter.or, [...path, 'or']);
    }

    return [{ filter, path }];
  });
}

function updateCollection(
  filters: readonly Filter[],
  path: readonly (number | 'and' | 'or')[],
  replacement: Filter | null
): Filter[] {
  const [rawIndex, rawOperator, ...rest] = path;
  if (typeof rawIndex !== 'number' || rawIndex < 0 || rawIndex >= filters.length) {
    return [...filters];
  }

  if (path.length === 1) {
    return replacement == null
      ? filters.filter((_, index) => index !== rawIndex)
      : filters.map((filter, index) =>
          index === rawIndex ? replacement : filter
        );
  }

  if (rawOperator !== 'and' && rawOperator !== 'or') {
    return [...filters];
  }

  const node = filters[rawIndex];
  let nodeChildren: Filter[];
  if (rawOperator === 'and') {
    if (!('and' in node)) {
      return [...filters];
    }
    nodeChildren = node.and;
  } else {
    if (!('or' in node)) {
      return [...filters];
    }
    nodeChildren = node.or;
  }

  const children = updateCollection(nodeChildren, rest, replacement);
  const nextNode: Filter | null = children.length
    ? ({ [rawOperator]: children } as Filter)
    : null;

  return nextNode == null
    ? filters.filter((_, index) => index !== rawIndex)
    : filters.map((filter, index) =>
        index === rawIndex ? nextNode : filter
      );
}

export function updateFilterAtPath(
  filters: readonly Filter[],
  path: FilterPath,
  replacement: Filter | null
): Filter[] {
  return sanitizeFilters(updateCollection(filters, path, replacement));
}

export function isUnaryOperator(value: string): value is UnaryOperator {
  return UNARY_OPERATORS.has(value as UnaryOperator);
}
