import React from 'react';
import { prop, uniqBy, equals, pick } from 'ramda';
import { ResultSet, moveItemInArray, defaultOrder, flattenFilters, getQueryMembers } from '@cubejs-client/core';
import QueryRenderer from './QueryRenderer.jsx';
import CubeContext from './CubeContext';

const granularities = [
  { name: undefined, title: 'w/o grouping' },
  { name: 'second', title: 'Second' },
  { name: 'minute', title: 'Minute' },
  { name: 'hour', title: 'Hour' },
  { name: 'day', title: 'Day' },
  { name: 'week', title: 'Week' },
  { name: 'month', title: 'Month' },
  { name: 'year', title: 'Year' },
];

export default class QueryBuilder extends React.Component {
  // This is an anti-pattern, only kept for backward compatibility
  // https://reactjs.org/blog/2018/06/07/you-probably-dont-need-derived-state.html#anti-pattern-unconditionally-copying-props-to-state
  static getDerivedStateFromProps(props, state) {
    if (props.query || props.vizState) {
      const nextState = {
        ...state,
        ...(props.vizState || {}),
      };

      if (Array.isArray(props.query)) {
        throw new Error('Array of queries is not supported.');
      }

      return {
        ...nextState,
        query: {
          ...nextState.query,
          ...(props.query || {}),
        },
      };
    }
    return null;
  }

  static resolveMember(type, { meta, query }) {
    if (!meta) {
      return [];
    }

    if (Array.isArray(query)) {
      return query.reduce((memo, currentQuery) => memo.concat(QueryBuilder.resolveMember(type, {
        meta,
        query: currentQuery
      })), []);
    }

    if (type === 'timeDimensions') {
      return (query.timeDimensions || []).map((m, index) => ({
        ...m,
        dimension: {
          ...meta.resolveMember(m.dimension, 'dimensions'),
          granularities,
        },
        index,
      }));
    }

    return (query[type] || []).map((m, index) => ({
      index,
      ...meta.resolveMember(m, type),
    }));
  }

  constructor(props) {
    super(props);

    this.state = {
      query: props.defaultQuery || props.query,
      chartType: props.defaultChartType,
      validatedQuery: props.query, // deprecated, validatedQuery should not be set until after dry-run for safety
      missingMembers: [],
      isFetchingMeta: false,
      ...props.vizState, // deprecated
      ...props.initialVizState
    };

    this.mutexObj = {};
  }

  async componentDidMount() {
    await this.fetchMeta();
  }

  async componentDidUpdate(prevProps) {
    const { schemaVersion, onSchemaChange } = this.props;
    const { meta } = this.state;

    if (prevProps.schemaVersion !== schemaVersion) {
      try {
        const newMeta = await this.cubejsApi().meta();
        if (!equals(newMeta, meta) && typeof onSchemaChange === 'function') {
          onSchemaChange({
            schemaVersion,
            refresh: async () => {
              await this.fetchMeta();
            }
          });
        }
      } catch (error) {
        // eslint-disable-next-line
        this.setState({ metaError: error });
      }
    }
  }

  fetchMeta = async () => {
    let meta;
    let metaError = null;

    try {
      this.setState({ isFetchingMeta: true });
      meta = await this.cubejsApi().meta();
    } catch (error) {
      metaError = error;
    }

    this.setState({
      meta,
      metaError,
      isFetchingMeta: false
    }, () => {
      // Run update query to force viz state update
      // This will catch any new missing members, and also validate the query against the new meta
      this.updateQuery({});
    });
  }

  cubejsApi() {
    const { cubejsApi } = this.props;
    // eslint-disable-next-line react/destructuring-assignment
    return cubejsApi || (this.context && this.context.cubejsApi);
  }

  getMissingMembers(query, meta) {
    if (!meta) {
      return [];
    }

    return getQueryMembers(query)
      .map((member) => {
        const resolvedMember = meta.resolveMember(member, ['measures', 'dimensions', 'segments']);
        if (resolvedMember.error) {
          return member;
        }
        return false;
      })
      .filter(Boolean);
  }

  isQueryPresent() {
    const { query } = this.state;
    return QueryRenderer.isQueryPresent(query);
  }

  prepareRenderProps(queryRendererProps) {
    const getName = (member) => member.name;
    const toTimeDimension = (member) => {
      const rangeSelection = member.compareDateRange
        ? { compareDateRange: member.compareDateRange }
        : { dateRange: member.dateRange };
      return {
        dimension: member.dimension.name,
        granularity: member.granularity,
        ...rangeSelection,
      };
    };
    const toFilter = (member) => ({
      member: member.member?.name || member.dimension?.name,
      operator: member.operator,
      values: member.values,
    });

    const updateMethods = (memberType, toQuery = getName) => ({
      add: (member) => {
        const { query } = this.state;
        this.updateQuery({
          [memberType]: (query[memberType] || []).concat(toQuery(member)),
        });
      },
      remove: (member) => {
        const { query } = this.state;
        const members = (query[memberType] || []).concat([]);
        members.splice(member.index, 1);
        return this.updateQuery({
          [memberType]: members,
        });
      },
      update: (member, updateWith) => {
        const { query } = this.state;
        const members = (query[memberType] || []).concat([]);
        members.splice(member.index, 1, toQuery(updateWith));
        return this.updateQuery({
          [memberType]: members,
        });
      },
    });

    const {
      meta,
      metaError,
      query,
      queryError,
      chartType,
      pivotConfig,
      validatedQuery,
      missingMembers,
      isFetchingMeta
    } = this.state;

    const flatFilters = uniqBy(
      prop('member'),
      flattenFilters((meta && query.filters) || []).map((filter) => ({
        ...filter,
        member: filter.member || filter.dimension,
      }))
    );

    const filters = flatFilters.map((m, i) => ({
      ...m,
      dimension: meta.resolveMember(m.member || m.dimension, ['dimensions', 'measures']),
      operators: meta.filterOperatorsForMember(m.member || m.dimension, ['dimensions', 'measures']),
      index: i,
    }));

    const measures = QueryBuilder.resolveMember('measures', this.state);
    const dimensions = QueryBuilder.resolveMember('dimensions', this.state);
    const timeDimensions = QueryBuilder.resolveMember('timeDimensions', this.state);
    const segments = ((meta && query.segments) || []).map((m, i) => ({
      index: i,
      ...meta.resolveMember(m, 'segments'),
    }));

    const availableMeasures = meta ? meta.membersForQuery(query, 'measures') : [];
    const availableDimensions = meta ? meta.membersForQuery(query, 'dimensions') : [];
    const availableSegments = meta ? meta.membersForQuery(query, 'segments') : [];

    let orderMembers = uniqBy(prop('id'), [
      ...(Array.isArray(query.order) ? query.order : Object.entries(query.order || {})).map(([id, order]) => ({
        id,
        order,
        title: meta ? meta.resolveMember(id, ['measures', 'dimensions']).title : '',
      })),
      // uniqBy prefers first, so these will only be added if not already in the query
      ...[...measures, ...dimensions].map(({ name, title }) => ({ id: name, title, order: 'none' })),
    ]);

    // Preserve order until the members change or manually re-ordered
    // This is needed so that when an order member becomes active, it doesn't jump to the top of the list
    const orderMemberOrderKey = JSON.stringify(orderMembers.map(({ id }) => id).sort());
    if (this.orderMemberOrderKey && this.orderMemberOrder && orderMemberOrderKey === this.orderMemberOrderKey) {
      orderMembers = this.orderMemberOrder.map((id) => orderMembers.find((member) => member.id === id));
    } else {
      this.orderMemberOrderKey = orderMemberOrderKey;
      this.orderMemberOrder = orderMembers.map(({ id }) => id);
    }

    return {
      meta,
      metaError,
      query,
      error: queryError, // Match same name as QueryRenderer prop
      validatedQuery,
      isQueryPresent: this.isQueryPresent(),
      chartType,
      measures,
      dimensions,
      timeDimensions,
      segments,
      filters,
      orderMembers,
      availableMeasures,
      availableDimensions,
      availableTimeDimensions: availableDimensions.filter((m) => m.type === 'time'),
      availableSegments,
      updateQuery: (queryUpdate) => this.updateQuery(queryUpdate),
      updateMeasures: updateMethods('measures'),
      updateDimensions: updateMethods('dimensions'),
      updateSegments: updateMethods('segments'),
      updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
      updateFilters: updateMethods('filters', toFilter),
      updateChartType: (newChartType) => this.updateVizState({ chartType: newChartType }),
      updateOrder: {
        set: (memberId, newOrder = 'asc') => {
          this.updateQuery({
            order: orderMembers
              .map((orderMember) => ({
                ...orderMember,
                order: orderMember.id === memberId ? newOrder : orderMember.order,
              }))
              .reduce((acc, { id, order }) => (order !== 'none' ? [...acc, [id, order]] : acc), []),
          });
        },
        update: (order) => {
          this.updateQuery({
            order,
          });
        },
        reorder: (sourceIndex, destinationIndex) => {
          if (sourceIndex == null || destinationIndex == null) {
            return;
          }

          this.updateQuery({
            order: moveItemInArray(orderMembers, sourceIndex, destinationIndex).reduce(
              (acc, { id, order }) => (order !== 'none' ? [...acc, [id, order]] : acc),
              []
            ),
          });
        },
      },
      pivotConfig,
      updatePivotConfig: {
        moveItem: ({ sourceIndex, destinationIndex, sourceAxis, destinationAxis }) => {
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

          this.updateVizState({
            pivotConfig: nextPivotConfig,
          });
        },
        update: (config) => {
          const { limit } = config;

          this.updateVizState({
            pivotConfig: {
              ...pivotConfig,
              ...config,
            },
            ...(limit ? { query: { ...query, limit } } : null)
          });
        },
      },
      missingMembers,
      refresh: this.fetchMeta,
      isFetchingMeta,
      ...queryRendererProps,
    };
  }

  updateQuery(queryUpdate) {
    const { query } = this.state;

    this.updateVizState({
      query: {
        ...query,
        ...queryUpdate,
      },
    });
  }

  async updateVizState(state) {
    const { setQuery, setVizState } = this.props;
    const { query: stateQuery, pivotConfig: statePivotConfig, meta } = this.state;

    const finalState = this.applyStateChangeHeuristics(state);
    if (!finalState.query) {
      finalState.query = { ...stateQuery };
    }

    const handleVizStateChange = (currentState) => {
      const { onVizStateChanged } = this.props;
      if (onVizStateChanged) {
        onVizStateChanged(pick(['chartType', 'pivotConfig', 'query'], currentState));
      }
    };

    // deprecated, setters replaced by onVizStateChanged
    const runSetters = (currentState) => {
      if (setVizState) {
        const { meta: _, validatedQuery, ...toSet } = currentState;
        setVizState(toSet);
      }
      if (currentState.query && setQuery) {
        setQuery(currentState.query);
      }
    };

    if (finalState.shouldApplyHeuristicOrder) {
      finalState.query.order = defaultOrder(finalState.query);
    }

    finalState.pivotConfig = ResultSet.getNormalizedPivotConfig(
      finalState.query,
      finalState.pivotConfig !== undefined ? finalState.pivotConfig : statePivotConfig
    );

    finalState.missingMembers = this.getMissingMembers(finalState.query, meta);

    // deprecated
    runSetters({
      ...state,
      query: finalState.query,
    });

    // Update optimistically so that UI does not stutter
    this.setState({
      ...finalState,
      queryError: null,
    });

    handleVizStateChange(finalState);

    if (QueryRenderer.isQueryPresent(finalState.query) && finalState.missingMembers.length === 0) {
      try {
        const response = await this.cubejsApi().dryRun(finalState.query, {
          mutexObj: this.mutexObj,
        });

        if (finalState.shouldApplyHeuristicOrder) {
          finalState.query.order = (response.queryOrder || []).reduce((memo, current) => ({ ...memo, ...current }), {});
        }

        finalState.pivotConfig = ResultSet.getNormalizedPivotConfig(response.pivotQuery, finalState.pivotConfig);
        finalState.validatedQuery = this.validatedQuery(finalState);

        // deprecated
        if (QueryRenderer.isQueryPresent(stateQuery)) {
          runSetters({
            ...this.state,
            ...finalState,
          });
        }
      } catch (error) {
        console.error(error);
        this.setState({
          queryError: error
        });
      }
    }

    this.setState(finalState, () => handleVizStateChange(this.state));
  }

  validatedQuery(state) {
    const { query } = state || this.state;

    return {
      ...query,
      filters: (query.filters || []).filter((f) => f.operator),
    };
  }

  defaultHeuristics(newState) {
    const { query, sessionGranularity, meta } = this.state;
    const defaultGranularity = sessionGranularity || 'day';

    if (Array.isArray(query)) {
      return newState;
    }

    if (newState.query) {
      const oldQuery = query;
      let newQuery = newState.query;

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
        ((oldQuery.measures || []).length === 0 && (newQuery.measures || []).length > 0)
        || ((oldQuery.measures || []).length === 1
          && (newQuery.measures || []).length === 1
          && oldQuery.measures[0] !== newQuery.measures[0])
      ) {
        const defaultTimeDimension = meta.defaultTimeDimensionNameFor(newQuery.measures[0]);
        newQuery = {
          ...newQuery,
          timeDimensions: defaultTimeDimension
            ? [
              {
                dimension: defaultTimeDimension,
                granularity: defaultGranularity,
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
            granularity: td.granularity || defaultGranularity,
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
        ((oldQuery.dimensions || []).length > 0 || (oldQuery.measures || []).length > 0)
        && (newQuery.dimensions || []).length === 0
        && (newQuery.measures || []).length === 0
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
        (newChartType === 'line' || newChartType === 'area')
        && (query.timeDimensions || []).length === 1
        && !query.timeDimensions[0].granularity
      ) {
        const [td] = query.timeDimensions;
        return {
          ...newState,
          pivotConfig: null,
          query: {
            ...query,
            timeDimensions: [{ ...td, granularity: defaultGranularity }],
          },
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
          pivotConfig: null,
          shouldApplyHeuristicOrder: true,
          query: {
            ...query,
            timeDimensions: [{ ...td, granularity: undefined }],
          },
        };
      }
    }

    return newState;
  }

  applyStateChangeHeuristics(newState) {
    const { stateChangeHeuristics, disableHeuristics } = this.props;
    if (disableHeuristics) {
      return newState;
    }
    return (stateChangeHeuristics && stateChangeHeuristics(this.state, newState)) || this.defaultHeuristics(newState);
  }

  render() {
    const { query } = this.state;
    const { cubejsApi, render, wrapWithQueryRenderer } = this.props;

    if (wrapWithQueryRenderer) {
      return (
        <QueryRenderer
          query={query}
          cubejsApi={cubejsApi}
          resetResultSetOnChange={false}
          render={(queryRendererProps) => {
            if (render) {
              return render(this.prepareRenderProps(queryRendererProps));
            }
            return null;
          }}
        />
      );
    } else {
      if (render) {
        return render(this.prepareRenderProps());
      }
      return null;
    }
  }
}

QueryBuilder.contextType = CubeContext;

QueryBuilder.defaultProps = {
  cubejsApi: null,
  stateChangeHeuristics: null,
  disableHeuristics: false,
  render: null,
  wrapWithQueryRenderer: true,
  defaultChartType: 'line',
  defaultQuery: {},
  initialVizState: null,
  onVizStateChanged: null,

  // deprecated
  query: null,
  setQuery: null,
  vizState: null,
  setVizState: null,
};
