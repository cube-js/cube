import { indexBy, prop, clone, equals, fromPairs, toPairs } from 'ramda';

export const DEFAULT_GRANULARITY = 'day';

export const GRANULARITIES = [
  { name: undefined, title: 'w/o grouping' },
  { name: 'second', title: 'Second' },
  { name: 'minute', title: 'Minute' },
  { name: 'hour', title: 'Hour' },
  { name: 'day', title: 'Day' },
  { name: 'week', title: 'Week' },
  { name: 'month', title: 'Month' },
  { name: 'quarter', title: 'Quarter' },
  { name: 'year', title: 'Year' },
];

export function removeEmptyQueryFields(_query) {
  const query = _query || {};
  
  return fromPairs(
    toPairs(query)
      .map(([key, value]) => {
        if (
          ['measures', 'dimensions', 'segments', 'timeDimensions', 'filters'].includes(key)
        ) {
          if (Array.isArray(value) && value.length === 0) {
            return null;
          }
        }
        
        if (key === 'order' && value) {
          if (Array.isArray(value) && !value.length) {
            return null;
          } else if (!Object.keys(value).length) {
            return null;
          }
        }

        return [key, value];
      })
      .filter(Boolean)
  );
}

export function validateQuery(_query) {
  const query = _query || {};
  
  return removeEmptyQueryFields({
    ...query,
    filters: (query.filters || []).filter((f) => f.operator),
    timeDimensions: (query.timeDimensions || []).filter(
      (td) => !(!td.dateRange && !td.granularity)
    ),
  });
}

export function areQueriesEqual(query1 = {}, query2 = {}) {
  return (
    equals(
      Object.entries((query1 && query1.order) || {}),
      Object.entries((query2 && query2.order) || {})
    ) && equals(query1, query2)
  );
}

export function defaultOrder(query) {
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
      [query.measures[0]]: 'desc',
    };
  } else if ((query.dimensions || []).length > 0) {
    return {
      [query.dimensions[0]]: 'asc',
    };
  }

  return {};
}

export function defaultHeuristics(newState, oldQuery = {}, options) {
  const { query, ...props } = clone(newState);
  const { meta, sessionGranularity } = options;
  const granularity = sessionGranularity || DEFAULT_GRANULARITY;

  let state = {
    query,
    ...props,
  };

  let newQuery = null;
  if (!areQueriesEqual(query, oldQuery)) {
    newQuery = query;
  }

  if (Array.isArray(newQuery) || Array.isArray(oldQuery)) {
    return newState;
  }

  if (newQuery) {
    if (
      (oldQuery.timeDimensions || []).length === 1 &&
      (newQuery.timeDimensions || []).length === 1 &&
      newQuery.timeDimensions[0].granularity &&
      oldQuery.timeDimensions[0].granularity !==
        newQuery.timeDimensions[0].granularity
    ) {
      state = {
        ...state,
        sessionGranularity: newQuery.timeDimensions[0].granularity,
      };
    }

    if (
      ((oldQuery.measures || []).length === 0 &&
        (newQuery.measures || []).length > 0) ||
      ((oldQuery.measures || []).length === 1 &&
        (newQuery.measures || []).length === 1 &&
        oldQuery.measures[0] !== newQuery.measures[0])
    ) {
      const [td] = newQuery.timeDimensions || [];
      const defaultTimeDimension = meta.defaultTimeDimensionNameFor(
        newQuery.measures[0]
      );
      newQuery = {
        ...newQuery,
        timeDimensions: defaultTimeDimension
          ? [
            {
              dimension: defaultTimeDimension,
              granularity: (td && td.granularity) || granularity,
              dateRange: td && td.dateRange,
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
      !oldQuery.timeDimensions[0].granularity
    ) {
      const [td] = oldQuery.timeDimensions;
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
      oldQuery.timeDimensions[0].granularity
    ) {
      const [td] = oldQuery.timeDimensions;
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

export function isQueryPresent(query) {
  if (!query) {
    return false;
  }

  return (Array.isArray(query) ? query : [query]).every(
    (q) => (q.measures && q.measures.length) ||
      (q.dimensions && q.dimensions.length) ||
      (q.timeDimensions && q.timeDimensions.length)
  );
}

export function movePivotItem(
  pivotConfig,
  sourceIndex,
  destinationIndex,
  sourceAxis,
  destinationAxis
) {
  const nextPivotConfig = {
    ...pivotConfig,
    x: [...pivotConfig.x],
    y: [...pivotConfig.y],
  };
  const id = pivotConfig[sourceAxis][sourceIndex];
  const lastIndex = nextPivotConfig[destinationAxis].length - 1;

  if (id === 'measures') {
    destinationIndex = lastIndex + 1;
  } else if (
    destinationIndex >= lastIndex &&
    nextPivotConfig[destinationAxis][lastIndex] === 'measures'
  ) {
    destinationIndex = lastIndex - 1;
  }

  nextPivotConfig[sourceAxis].splice(sourceIndex, 1);
  nextPivotConfig[destinationAxis].splice(destinationIndex, 0, id);

  return nextPivotConfig;
}

export function moveItemInArray(list, sourceIndex, destinationIndex) {
  const result = [...list];
  const [removed] = result.splice(sourceIndex, 1);
  result.splice(destinationIndex, 0, removed);

  return result;
}

export function flattenFilters(filters = []) {
  return filters.reduce((memo, filter) => {
    if (filter.or || filter.and) {
      return [...memo, ...flattenFilters(filter.or || filter.and)];
    }

    return [...memo, filter];
  }, []);
}

export function getQueryMembers(query = {}) {
  const keys = ['measures', 'dimensions', 'segments'];
  const members = new Set();

  keys.forEach((key) => (query[key] || []).forEach((member) => members.add(member)));
  (query.timeDimensions || []).forEach((td) => members.add(td.dimension));

  flattenFilters(query.filters).forEach((filter) => members.add(filter.dimension || filter.member));

  return [...members];
}

export function getOrderMembersFromOrder(orderMembers, order) {
  const ids = new Set();
  const indexedOrderMembers = indexBy(prop('id'), orderMembers);
  const entries = Array.isArray(order) ? order : Object.entries(order || {});
  const nextOrderMembers = [];

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

export function aliasSeries(values, index, pivotConfig, duplicateMeasures) {
  const nonNullValues = values.filter((value) => value != null);

  if (
    pivotConfig &&
    pivotConfig.aliasSeries &&
    pivotConfig.aliasSeries[index]
  ) {
    return [pivotConfig.aliasSeries[index], ...nonNullValues];
  } else if (duplicateMeasures.has(nonNullValues[0])) {
    return [index, ...nonNullValues];
  }

  return nonNullValues;
}
