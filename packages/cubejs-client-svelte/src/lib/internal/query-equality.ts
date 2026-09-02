import { areQueriesEqual, type Query } from '@cubejs-client/core';

import type {
  CubeQueryOptions,
  NamedQueries,
  QueryInput,
  QueryRendererInput,
  QueryRendererOptions,
  VizState,
} from '../types';

export function deepEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) {
    return true;
  }

  if (typeof a !== typeof b || a == null || b == null) {
    return false;
  }

  if (Array.isArray(a) || Array.isArray(b)) {
    return (
      Array.isArray(a) &&
      Array.isArray(b) &&
      a.length === b.length &&
      a.every((value, index) => deepEqual(value, b[index]))
    );
  }

  if (typeof a === 'object' && typeof b === 'object') {
    const left = a as Record<string, unknown>;
    const right = b as Record<string, unknown>;
    const leftKeys = Object.keys(left);
    const rightKeys = Object.keys(right);

    return (
      leftKeys.length === rightKeys.length &&
      leftKeys.every(
        (key) =>
          Object.prototype.hasOwnProperty.call(right, key) &&
          deepEqual(left[key], right[key])
      )
    );
  }

  return false;
}

export function areQueryInputsEqual(
  left: QueryInput | null | undefined,
  right: QueryInput | null | undefined
): boolean {
  if (left === right) {
    return true;
  }

  if (!left || !right) {
    return false;
  }

  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((query, index) =>
        areQueriesEqual(query, right[index] as Query)
      )
    );
  }

  return areQueriesEqual(left as Query, right as Query);
}

export function areNamedQueriesEqual(
  left: NamedQueries,
  right: NamedQueries
): boolean {
  const leftNames = Object.keys(left);
  const rightNames = Object.keys(right);

  return (
    leftNames.length === rightNames.length &&
    leftNames.every(
      (name) =>
        Object.prototype.hasOwnProperty.call(right, name) &&
        areQueriesEqual(left[name], right[name])
    )
  );
}

export function areCubeQueryOptionsEqual(
  left: CubeQueryOptions,
  right: CubeQueryOptions
): boolean {
  return (
    left.cubeApi === right.cubeApi &&
    left.skip === right.skip &&
    left.subscribe === right.subscribe &&
    left.resetResultSetOnChange === right.resetResultSetOnChange &&
    left.castNumerics === right.castNumerics &&
    left.cache === right.cache &&
    left.baseRequestId === right.baseRequestId
  );
}

export function areRendererOptionsEqual(
  left: QueryRendererOptions,
  right: QueryRendererOptions
): boolean {
  return (
    areCubeQueryOptionsEqual(left, right) && left.loadSql === right.loadSql
  );
}

export function areRendererInputsEqual(
  left: QueryRendererInput,
  right: QueryRendererInput
): boolean {
  if ('query' in left && left.query !== undefined) {
    return (
      'query' in right &&
      right.query !== undefined &&
      areQueryInputsEqual(left.query, right.query)
    );
  }

  return (
    'queries' in right &&
    right.queries !== undefined &&
    areNamedQueriesEqual(left.queries, right.queries)
  );
}

export function areVizStatesEqual(left: VizState, right: VizState): boolean {
  return (
    areQueriesEqual(left.query, right.query) &&
    left.chartType === right.chartType &&
    deepEqual(left.pivotConfig, right.pivotConfig)
  );
}
