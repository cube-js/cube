export default (oldState, newState) => {
    const { query, sessionGranularity } = oldState;
    const defaultGranularity = sessionGranularity || 'day';
    if (newState.query) {
      const oldQuery = query;
      let newQuery = newState.query;

      const { meta } = oldState;

      if (
        (oldQuery.timeDimensions || []).length === 1
        && (newQuery.timeDimensions || []).length === 1
        && newQuery.timeDimensions[0].granularity
        && oldQuery.timeDimensions[0].granularity !== newQuery.timeDimensions[0].granularity
      ) {
        newState = {
          ...newState,
          sessionGranularity: newQuery.timeDimensions[0].granularity,
        };
      }

      if (
        // eslint-disable-next-line no-mixed-operators
        (oldQuery.measures || []).length === 0 && (newQuery.measures || []).length > 0
        // eslint-disable-next-line no-mixed-operators
        || (
          (oldQuery.measures || []).length === 1
          && (newQuery.measures || []).length === 1
          && oldQuery.measures[0] !== newQuery.measures[0]
        )
      ) {
        const defaultTimeDimension = meta.defaultTimeDimensionNameFor(newQuery.measures[0]);
        newQuery = {
          ...newQuery,
          timeDimensions: defaultTimeDimension ? [{
            dimension: defaultTimeDimension,
            granularity: defaultGranularity,
            dateRange: "Last 30 days"
          }] : [],
        };
        return {
          ...newState,
          query: newQuery,
          chartType: defaultTimeDimension ? 'line' : 'number'
        };
      }

      if (
        (oldQuery.dimensions || []).length === 0
        && (newQuery.dimensions || []).length > 0
      ) {
        newQuery = {
          ...newQuery,
          timeDimensions: (newQuery.timeDimensions || []).map(td => ({ ...td, granularity: undefined })),
        };
        return {
          ...newState,
          query: newQuery,
          chartType: 'table'
        };
      }

      if (
        (oldQuery.dimensions || []).length > 0
        && (newQuery.dimensions || []).length === 0
      ) {
        newQuery = {
          ...newQuery,
          timeDimensions: (newQuery.timeDimensions || []).map(td => ({
            ...td, granularity: td.granularity || defaultGranularity
          })),
        };
        return {
          ...newState,
          query: newQuery,
          chartType: (newQuery.timeDimensions || []).length ? 'line' : 'number'
        };
      }

      if (
        (
          (oldQuery.dimensions || []).length > 0
          || (oldQuery.measures || []).length > 0
        )
        && (newQuery.dimensions || []).length === 0
        && (newQuery.measures || []).length === 0
      ) {
        newQuery = {
          ...newQuery,
          timeDimensions: [],
          filters: []
        };
        return {
          ...newState,
          query: newQuery,
          sessionGranularity: null
        };
      }
      return newState;
    }

    if (newState.chartType) {
      const newChartType = newState.chartType;
      if (
        (newChartType === 'line' || newChartType === 'area')
        && (query.timeDimensions || []).length === 1
        && !query.timeDimensions[0].granularity
      ) {
        const [td] = query.timeDimensions;
        return {
          ...newState,
          query: {
            ...query,
            timeDimensions: [{ ...td, granularity: defaultGranularity }]
          }
        };
      }

      if (
        (newChartType === 'pie' || newChartType === 'table' || newChartType === 'number')
        && (query.timeDimensions || []).length === 1
        && query.timeDimensions[0].granularity
      ) {
        const [td] = query.timeDimensions;
        return {
          ...newState,
          query: {
            ...query,
            timeDimensions: [{ ...td, granularity: undefined }]
          }
        };
      }
    }

    return newState;
  }
