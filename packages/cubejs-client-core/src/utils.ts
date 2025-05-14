import { clone, equals, fromPairs, indexBy, prop, toPairs } from 'ramda';
import { DeeplyReadonly } from './index';
import { DEFAULT_GRANULARITY } from './time';
import {
  Filter,
  PivotConfig,
  Query,
  QueryOrder,
  TDefaultHeuristicsOptions,
  TDefaultHeuristicsResponse,
  TDefaultHeuristicsState,
  TFlatFilter,
  TOrderMember,
  TQueryOrderArray,
  TQueryOrderObject,
  TSourceAxis
} from './types';

export function removeEmptyQueryFields(_query: DeeplyReadonly<Query>) {
  const query = _query || {};

  return fromPairs(
    toPairs(query).flatMap(([key, value]) => {
      if (
        ['measures', 'dimensions', 'segments', 'timeDimensions', 'filters'].includes(key)
      ) {
        if (Array.isArray(value) && value.length === 0) {
          return [];
        }
      }

      if (key === 'order' && value) {
        if (Array.isArray(value) && value.length === 0) {
          return [];
        } else if (!Object.keys(value).length) {
          return [];
        }
      }

      return [[key, value]];
    })
  );
}

export function validateQuery(_query: DeeplyReadonly<Query> | null | undefined): Query {
  const query = _query || {};

  return removeEmptyQueryFields({
    ...query,
    filters: (query.filters || []).filter((f) => 'operator' in f),
    timeDimensions: (query.timeDimensions || []).filter(
      (td) => !(!td.dateRange && !td.granularity)
    ),
  });
}

export function areQueriesEqual(query1: DeeplyReadonly<Query> | null, query2: DeeplyReadonly<Query> | null): boolean {
  return (
    equals(
      Object.entries(query1?.order || {}),
      Object.entries(query2?.order || {})
    ) && equals(query1, query2)
  );
}

export function defaultOrder(query: DeeplyReadonly<Query>): { [key: string]: QueryOrder } {
  const granularity = (query.timeDimensions || []).find((d) => d.granularity);

  if (granularity) {
    return {
      [granularity.dimension]: 'asc',
    };
  } else if (
    (query.measures || []).length > 0 &&
    (query.dimensions || []).length > 0
  ) {
    return {
      [query.measures![0]]: 'desc',
    };
  } else if ((query.dimensions || []).length > 0) {
    return {
      [query.dimensions![0]]: 'asc',
    };
  }

  return {};
}

export function defaultHeuristics(
  newState: TDefaultHeuristicsState,
  oldQuery: Query,
  options: TDefaultHeuristicsOptions
): TDefaultHeuristicsResponse {
  const { query, ...props } = clone(newState);
  const { meta, sessionGranularity } = options;
  const granularity = sessionGranularity || DEFAULT_GRANULARITY;

  let state: TDefaultHeuristicsResponse = {
    shouldApplyHeuristicOrder: false,
    pivotConfig: null,
    query,
    ...props,
  };

  let newQuery = null;
  if (!areQueriesEqual(query, oldQuery)) {
    newQuery = query;
  }

  if (Array.isArray(newQuery) || Array.isArray(oldQuery)) {
    return {
      shouldApplyHeuristicOrder: false,
      pivotConfig: null,
      ...newState,
    };
  }

  if (newQuery) {
    if (
      (oldQuery.timeDimensions || []).length === 1 &&
      (newQuery.timeDimensions || []).length === 1 &&
      newQuery.timeDimensions![0].granularity &&
      oldQuery.timeDimensions![0].granularity !==
        newQuery.timeDimensions![0].granularity
    ) {
      state = {
        ...state,
        sessionGranularity: newQuery.timeDimensions![0].granularity,
      };
    }

    if (
      ((oldQuery.measures || []).length === 0 &&
        (newQuery.measures || []).length > 0) ||
      ((oldQuery.measures || []).length === 1 &&
        (newQuery.measures || []).length === 1 &&
        oldQuery.measures![0] !== newQuery.measures![0])
    ) {
      const [td] = newQuery.timeDimensions || [];
      const defaultTimeDimension = meta.defaultTimeDimensionNameFor(
        newQuery.measures![0]
      );
      newQuery = {
        ...newQuery,
        timeDimensions: defaultTimeDimension
          ? [
            {
              dimension: defaultTimeDimension,
              granularity: td?.granularity || granularity,
              dateRange: td?.dateRange,
            },
          ]
          : [],
      };

      return {
        ...state,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: newQuery,
        chartType: defaultTimeDimension ? 'line' : 'number',
      };
    }

    if (
      (oldQuery.dimensions || []).length === 0 &&
      (newQuery.dimensions || []).length > 0
    ) {
      newQuery = {
        ...newQuery,
        timeDimensions: (newQuery.timeDimensions || []).map((td) => ({
          ...td,
          granularity: undefined,
        })),
      };

      return {
        ...state,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: newQuery,
        chartType: 'table',
      };
    }

    if (
      (oldQuery.dimensions || []).length > 0 &&
      (newQuery.dimensions || []).length === 0
    ) {
      newQuery = {
        ...newQuery,
        timeDimensions: (newQuery.timeDimensions || []).map((td) => ({
          ...td,
          granularity: td.granularity || granularity,
        })),
      };

      return {
        ...state,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: newQuery,
        chartType: (newQuery.timeDimensions || []).length ? 'line' : 'number',
      };
    }

    if (
      ((oldQuery.dimensions || []).length > 0 ||
        (oldQuery.measures || []).length > 0) &&
      (newQuery.dimensions || []).length === 0 &&
      (newQuery.measures || []).length === 0
    ) {
      newQuery = {
        ...newQuery,
        timeDimensions: [],
        filters: [],
      };

      return {
        ...state,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: newQuery,
        sessionGranularity: null,
      };
    }
    return state;
  }

  if (state.chartType) {
    const newChartType = state.chartType;
    if (
      (newChartType === 'line' || newChartType === 'area') &&
      (oldQuery.timeDimensions || []).length === 1 &&
      !oldQuery.timeDimensions![0].granularity
    ) {
      const [td] = oldQuery.timeDimensions!;
      return {
        ...state,
        pivotConfig: null,
        query: {
          ...oldQuery,
          timeDimensions: [{ ...td, granularity }],
        },
      };
    }

    if (
      (newChartType === 'pie' ||
        newChartType === 'table' ||
        newChartType === 'number') &&
      (oldQuery.timeDimensions || []).length === 1 &&
      oldQuery.timeDimensions![0].granularity
    ) {
      const [td] = oldQuery.timeDimensions!;
      return {
        ...state,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: {
          ...oldQuery,
          timeDimensions: [{ ...td, granularity: undefined }],
        },
      };
    }
  }

  return state;
}

export function isQueryPresent(query: DeeplyReadonly<Query | Query[]> | null | undefined): boolean {
  if (!query) {
    return false;
  }

  return (Array.isArray(query) ? query : [query]).every(
    (q) => q.measures?.length || q.dimensions?.length || q.timeDimensions?.length
  );
}

export function movePivotItem(
  pivotConfig: PivotConfig,
  sourceIndex: number,
  destinationIndex: number,
  sourceAxis: TSourceAxis,
  destinationAxis: TSourceAxis
): PivotConfig {
  const nextPivotConfig = {
    ...pivotConfig,
    x: [...(pivotConfig.x || [])],
    y: [...(pivotConfig.y || [])],
  };
  const id = pivotConfig[sourceAxis]![sourceIndex];
  const lastIndex = nextPivotConfig[destinationAxis].length - 1;

  if (id === 'measures') {
    destinationIndex = lastIndex + 1;
  } else if (
    sourceAxis === destinationAxis &&
    destinationIndex >= lastIndex &&
    nextPivotConfig[destinationAxis][lastIndex] === 'measures'
  ) {
    destinationIndex = lastIndex - 1;
  } else if (
    sourceAxis !== destinationAxis &&
    destinationIndex > lastIndex &&
    nextPivotConfig[destinationAxis][lastIndex] === 'measures'
  ) {
    destinationIndex = lastIndex;
  }

  nextPivotConfig[sourceAxis].splice(sourceIndex, 1);
  nextPivotConfig[destinationAxis].splice(destinationIndex, 0, id);

  return nextPivotConfig;
}

export function moveItemInArray<T = any>(list: T[], sourceIndex: number, destinationIndex: number): T[] {
  const result = [...list];
  const [removed] = result.splice(sourceIndex, 1);
  result.splice(destinationIndex, 0, removed);

  return result;
}

export function flattenFilters(filters: Filter[] = []): TFlatFilter[] {
  return filters.reduce<TFlatFilter[]>((memo, filter) => {
    if ('or' in filter) {
      return [...memo, ...flattenFilters(filter.or)];
    }

    if ('and' in filter) {
      return [...memo, ...flattenFilters(filter.and)];
    }

    return [...memo, filter];
  }, []);
}

export function getQueryMembers(query: DeeplyReadonly<Query> = {}): string[] {
  const keys = ['measures', 'dimensions', 'segments'] as const;
  const members = new Set<string>();

  keys.forEach((key) => (query[key] || []).forEach((member) => members.add(member)));
  (query.timeDimensions || []).forEach((td) => members.add(td.dimension));

  const filters = flattenFilters(query.filters as Filter[]);
  filters.forEach((filter) => {
    const member = filter.dimension || filter.member;
    if (typeof member === 'string') {
      members.add(member);
    }
  });

  return [...members];
}

export function getOrderMembersFromOrder(orderMembers: any[], order: TQueryOrderObject | TQueryOrderArray): TOrderMember[] {
  const ids = new Set<string>();
  const indexedOrderMembers = indexBy(prop('id'), orderMembers);
  const entries = Array.isArray(order) ? order : Object.entries(order || {});
  const nextOrderMembers: TOrderMember[] = [];

  entries.forEach(([memberId, currentOrder]) => {
    if (currentOrder !== 'none' && indexedOrderMembers[memberId]) {
      ids.add(memberId);
      nextOrderMembers.push({
        ...indexedOrderMembers[memberId],
        order: currentOrder,
      });
    }
  });
  orderMembers.forEach((member) => {
    if (!ids.has(member.id)) {
      nextOrderMembers.push({
        ...member,
        order: member.order || 'none',
      });
    }
  });

  return nextOrderMembers;
}

export function aliasSeries(values: string[], index: number, pivotConfig?: Partial<PivotConfig>, duplicateMeasures: Set<string> = new Set()) {
  const nonNullValues = values.filter((value: any) => value != null);

  if (pivotConfig?.aliasSeries?.[index]) {
    return [pivotConfig.aliasSeries[index], ...nonNullValues];
  } else if (duplicateMeasures.has(nonNullValues[0])) {
    return [index, ...nonNullValues];
  }

  return nonNullValues;
}
