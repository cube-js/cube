export const DEFAULT_GRANULARITY = 'day';

export function defaultOrder(query) {
  const granularity = (query.timeDimensions || []).find((d) => d.granularity);

  if (granularity) {
    return {
      [granularity.dimension]: 'asc'
    };
  } else if ((query.measures || []).length > 0 && (query.dimensions || []).length > 0) {
    return {
      [query.measures[0]]: 'desc'
    };
  } else if ((query.dimensions || []).length > 0) {
    return {
      [query.dimensions[0]]: 'asc'
    };
  }

  return {};
}

export function defaultHeuristics(newQuery, oldQuery = {}, options) {
  const { meta, sessionGranularity } = options;
  const granularity = sessionGranularity || DEFAULT_GRANULARITY;
  let newState = {};

  if (Array.isArray(newQuery) || Array.isArray(oldQuery)) {
    return newQuery;
  }

  if (newQuery) {
    if (
      (oldQuery.timeDimensions || []).length === 1 &&
      (newQuery.timeDimensions || []).length === 1 &&
      newQuery.timeDimensions[0].granularity &&
      oldQuery.timeDimensions[0].granularity !== newQuery.timeDimensions[0].granularity
    ) {
      newState = {
        ...newState,
        sessionGranularity: newQuery.timeDimensions[0].granularity,
      };
    }

    if (
      ((oldQuery.measures || []).length === 0 && (newQuery.measures || []).length > 0) ||
      ((oldQuery.measures || []).length === 1 &&
        (newQuery.measures || []).length === 1 &&
        oldQuery.measures[0] !== newQuery.measures[0])
    ) {
      const defaultTimeDimension = meta.defaultTimeDimensionNameFor(newQuery.measures[0]);
      newQuery = {
        ...newQuery,
        timeDimensions: defaultTimeDimension
          ? [
            {
              dimension: defaultTimeDimension,
              granularity,
            },
          ]
          : [],
      };

      return {
        ...newState,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: newQuery,
        chartType: defaultTimeDimension ? 'line' : 'number',
      };
    }

    if ((oldQuery.dimensions || []).length === 0 && (newQuery.dimensions || []).length > 0) {
      newQuery = {
        ...newQuery,
        timeDimensions: (newQuery.timeDimensions || []).map((td) => ({ ...td, granularity: undefined })),
      };

      return {
        ...newState,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: newQuery,
        chartType: 'table',
      };
    }

    if ((oldQuery.dimensions || []).length > 0 && (newQuery.dimensions || []).length === 0) {
      newQuery = {
        ...newQuery,
        timeDimensions: (newQuery.timeDimensions || []).map((td) => ({
          ...td,
          granularity: td.granularity || granularity,
        })),
      };

      return {
        ...newState,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: newQuery,
        chartType: (newQuery.timeDimensions || []).length ? 'line' : 'number',
      };
    }

    if (
      ((oldQuery.dimensions || []).length > 0 || (oldQuery.measures || []).length > 0) &&
      (newQuery.dimensions || []).length === 0 &&
      (newQuery.measures || []).length === 0
    ) {
      newQuery = {
        ...newQuery,
        timeDimensions: [],
        filters: [],
      };

      return {
        ...newState,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: newQuery,
        sessionGranularity: null,
      };
    }
    return newState;
  }

  if (newState.chartType) {
    const newChartType = newState.chartType;
    if (
      (newChartType === 'line' || newChartType === 'area') &&
      (oldQuery.timeDimensions || []).length === 1 &&
      !oldQuery.timeDimensions[0].granularity
    ) {
      const [td] = oldQuery.timeDimensions;
      return {
        ...newState,
        pivotConfig: null,
        query: {
          ...oldQuery,
          timeDimensions: [{ ...td, granularity }],
        },
      };
    }

    if (
      (newChartType === 'pie' || newChartType === 'table' || newChartType === 'number') &&
      (oldQuery.timeDimensions || []).length === 1 &&
      oldQuery.timeDimensions[0].granularity
    ) {
      const [td] = oldQuery.timeDimensions;
      return {
        ...newState,
        pivotConfig: null,
        shouldApplyHeuristicOrder: true,
        query: {
          ...oldQuery,
          timeDimensions: [{ ...td, granularity: undefined }],
        },
      };
    }
  }

  return newState;
}

export function isQueryPresent(query) {
  return (Array.isArray(query) ? query : [query]).every(
    (q) => (q.measures && q.measures.length) ||
      (q.dimensions && q.dimensions.length) ||
      (q.timeDimensions && q.timeDimensions.length)
  );
}

export function movePivotItem(pivotConfig, sourceIndex, destinationIndex, sourceAxis, destinationAxis) {
  const nextPivotConfig = {
    ...pivotConfig,
    x: [...pivotConfig.x],
    y: [...pivotConfig.y],
  };
  const id = pivotConfig[sourceAxis][sourceIndex];
  const lastIndex = nextPivotConfig[destinationAxis].length - 1;

  if (id === 'measures') {
    destinationIndex = lastIndex + 1;
  } else if (destinationIndex >= lastIndex && nextPivotConfig[destinationAxis][lastIndex] === 'measures') {
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
      return [
        ...memo,
        ...flattenFilters(filter.or || filter.and)
      ];
    }
    
    return [...memo, filter];
  }, []);
}
