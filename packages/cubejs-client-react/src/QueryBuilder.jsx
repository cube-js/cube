import React from 'react';
import { clone, equals, indexBy, pick, prop, uniq, uniqBy } from 'ramda';
import {
  defaultHeuristics,
  defaultOrder,
  flattenFilters,
  getQueryMembers,
  isQueryPresent,
  moveItemInArray,
  movePivotItem,
  validateQuery,
  ResultSet,
  removeEmptyQueryFields
} from '@cubejs-client/core';

import QueryRenderer from './QueryRenderer.jsx';
import CubeContext from './CubeContext';
import { removeEmpty } from './utils';

const granularities = [
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

export default class QueryBuilder extends React.Component {
  static contextType = CubeContext;

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
      return query.reduce(
        (memo, currentQuery) => memo.concat(
          QueryBuilder.resolveMember(type, {
            meta,
            query: currentQuery,
          })
        ),
        []
      );
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
      // todo: rename to `isMetaReady`
      isFetchingMeta: true,
      dryRunResponse: null,
      ...props.vizState, // deprecated
      ...props.initialVizState,
    };

    this.mutexObj = {};
    this.orderMembersOrderKeys = [];
  }

  async componentDidMount() {
    this.prevContext = this.context;
    await this.fetchMeta();
  }

  async componentDidUpdate(prevProps) {
    const { schemaVersion, onSchemaChange } = this.props;
    const { meta } = this.state;

    if (this.prevContext?.cubejsApi !== this.context?.cubejsApi) {
      this.prevContext = this.context;
      await this.fetchMeta();
    }

    if (prevProps.schemaVersion !== schemaVersion) {
      try {
        const newMeta = await this.cubejsApi().meta();
        if (!equals(newMeta, meta) && typeof onSchemaChange === 'function') {
          onSchemaChange({
            schemaVersion,
            refresh: async () => {
              await this.fetchMeta();
            },
          });
        }
      } catch (error) {
        // eslint-disable-next-line
        this.setState({ metaError: error });
      }
    }
  }

  fetchMeta = async () => {
    if (!this.cubejsApi()) {
      return;
    }

    let meta;
    let metaError = null;
    let richMetaError = null;
    let metaErrorStack = null;

    try {
      this.setState({ isFetchingMeta: true });
      meta = await this.cubejsApi().meta();
    } catch (error) {
      metaError = error.response?.plainError || error;
      richMetaError = error;
      metaErrorStack = error.response?.stack?.replace(error.message || '', '') || '';
    }

    this.setState(
      {
        meta,
        metaError: metaError ? new Error(metaError.message || metaError.toString()) : null,
        richMetaError,
        metaErrorStack,
        isFetchingMeta: false,
      },
      () => {
        // Run update query to force viz state update
        // This will catch any new missing members, and also validate the query against the new meta
        this.updateQuery({});
      }
    );
  };

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

      return removeEmpty({
        dimension: member.dimension.name,
        granularity: member.granularity,
        ...rangeSelection,
      });
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

        return this.updateQuery({
          [memberType]: (query[memberType] || []).filter((_, index) => index !== member.index),
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
      richMetaError,
      query,
      queryError,
      chartType,
      pivotConfig,
      validatedQuery,
      missingMembers,
      isFetchingMeta,
      dryRunResponse,
      metaErrorStack
    } = this.state;

    const flatFilters = uniqBy((filter) => `${prop('member', filter)}${prop('operator', filter)}`,
      flattenFilters((meta && query.filters) || []).map((filter) => ({
        ...filter,
        member: filter.member || filter.dimension,
      })));

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

    let availableMeasures = [];
    let availableDimensions = [];
    let availableSegments = [];
    let availableFilterMembers = [];

    const availableMembers = meta?.membersGroupedByCube() || {
      measures: [],
      dimensions: [],
      segments: [],
      timeDimensions: [],
    };

    if (meta) {
      availableMeasures = meta.membersForQuery(query, 'measures');
      availableDimensions = meta.membersForQuery(query, 'dimensions');
      availableSegments = meta.membersForQuery(query, 'segments');

      const indexedMeasures = indexBy(prop('cubeName'), availableMembers.measures);
      const indexedDimensions = indexBy(prop('cubeName'), availableMembers.dimensions);
      const cubeNames = uniq([...Object.keys(indexedMeasures), ...Object.keys(indexedDimensions)]).sort();

      availableFilterMembers = cubeNames.map((name) => {
        const cube = indexedMeasures[name] || indexedDimensions[name];

        return {
          ...cube,
          members: [
            ...indexedMeasures[name]?.members,
            ...indexedDimensions[name]?.members
          ].sort((a, b) => (a.shortTitle > b.shortTitle ? 1 : -1)),
        };
      });
    }

    const activeOrder = Array.isArray(query.order) ? Object.fromEntries(query.order) : query.order;
    const members = [
      ...measures,
      ...dimensions,
      ...timeDimensions.map(({ dimension }) => dimension)
    ];

    let orderMembers = uniqBy(prop('id'), [
      // uniqBy prefers first, so these will only be added if not already in the query
      ...members.map(({ name, title }) => ({ id: name, title, order: activeOrder?.[name] || 'none' })),
    ]);

    if (this.orderMembersOrderKeys.length !== orderMembers.length) {
      this.orderMembersOrderKeys = orderMembers.map(({ id }) => id);
    }

    if (this.orderMembersOrderKeys.length) {
      // Preserve order until the members change or manually re-ordered
      // This is needed so that when an order member becomes active, it doesn't jump to the top of the list
      orderMembers = (this.orderMembersOrderKeys || []).map((id) => orderMembers.find((member) => member.id === id));
    }

    return {
      meta,
      metaError,
      richMetaError,
      metaErrorStack,
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
      availableMembers,
      availableFilterMembers,
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

          const nextArray = moveItemInArray(orderMembers, sourceIndex, destinationIndex);
          this.orderMembersOrderKeys = nextArray.map(({ id }) => id);

          this.updateQuery({
            order: nextArray.reduce((acc, { id, order }) => (order !== 'none' ? [...acc, [id, order]] : acc), []),
          });
        },
      },
      pivotConfig,
      updatePivotConfig: {
        moveItem: ({ sourceIndex, destinationIndex, sourceAxis, destinationAxis }) => {
          this.updateVizState({
            pivotConfig: movePivotItem(pivotConfig, sourceIndex, destinationIndex, sourceAxis, destinationAxis),
          });
        },
        update: (config) => {
          const { limit } = config;

          this.updateVizState({
            pivotConfig: {
              ...pivotConfig,
              ...config,
            },
            ...(limit ? { query: { ...query, limit } } : null),
          });
        },
      },
      missingMembers,
      refresh: this.fetchMeta,
      isFetchingMeta,
      dryRunResponse,
      ...queryRendererProps,
    };
  }

  updateQuery(queryUpdate) {
    const { query } = this.state;

    this.updateVizState({
      query: removeEmptyQueryFields({
        ...query,
        ...queryUpdate,
      }),
    });
  }

  async updateVizState(state) {
    const { setQuery, setVizState } = this.props;
    const { query: stateQuery, pivotConfig: statePivotConfig, chartType, meta } = this.state;

    const finalState = this.applyStateChangeHeuristics(state);
    if (!finalState.query) {
      finalState.query = { ...stateQuery };
    }

    let vizStateSent = null;
    const handleVizStateChange = (currentState) => {
      const { onVizStateChanged } = this.props;
      if (onVizStateChanged) {
        const newVizState = pick(['chartType', 'pivotConfig', 'query'], currentState);
        // Don't run callbacks more than once unless the viz state has changed since last time
        if (!vizStateSent || !equals(vizStateSent, newVizState)) {
          onVizStateChanged(newVizState);
          // use clone to make sure we don't save object references
          vizStateSent = clone(newVizState);
        }
      }
    };

    // deprecated, setters replaced by onVizStateChanged
    const runSetters = (currentState) => {
      if (setVizState) {
        setVizState(pick(['chartType', 'pivotConfig', 'query'], currentState));
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
    finalState.chartType = finalState.chartType || state.chartType || chartType;

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

    const shouldFetchDryRun = !equals(
      pick(['measures', 'dimensions', 'timeDimensions'], stateQuery),
      pick(['measures', 'dimensions', 'timeDimensions'], finalState.query)
    );

    if (shouldFetchDryRun && isQueryPresent(finalState.query) && finalState.missingMembers.length === 0) {
      try {
        const response = await this.cubejsApi().dryRun(finalState.query, {
          mutexObj: this.mutexObj,
        });

        if (finalState.shouldApplyHeuristicOrder) {
          finalState.query.order = (response.queryOrder || []).reduce((memo, current) => ({ ...memo, ...current }), {});
        }

        finalState.pivotConfig = ResultSet.getNormalizedPivotConfig(response.pivotQuery, finalState.pivotConfig);
        finalState.validatedQuery = this.validatedQuery(finalState);
        finalState.dryRunResponse = response;

        // deprecated
        if (isQueryPresent(stateQuery)) {
          runSetters({
            ...this.state,
            ...finalState,
          });
        }
      } catch (error) {
        this.setState({
          queryError: new Error(error.response?.plainError || error.message),
          richQueryError: new Error(error.message || error.toString())
        });
      }
    }

    this.setState(finalState, () => handleVizStateChange(this.state));
  }

  validatedQuery(state) {
    const { query } = state || this.state;

    return validateQuery(query);
  }

  defaultHeuristics(newState) {
    const { query, sessionGranularity, meta } = this.state;

    return defaultHeuristics(newState, query, {
      meta,
      sessionGranularity: sessionGranularity || 'day',
    });
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
