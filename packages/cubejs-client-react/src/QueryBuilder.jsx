import React from 'react';
import {
  prop, uniqBy, indexBy, fromPairs
} from 'ramda';
import {
  ResultSet, moveItemInArray, defaultOrder, flattenFilters
} from '@cubejs-client/core';
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
  { name: 'year', title: 'Year' }
];

export default class QueryBuilder extends React.Component {
  static getDerivedStateFromProps(props, state) {
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
        ...(props.query || {})
      },
    };
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
          granularities
        },
        index
      }));
    }

    return (query[type] || []).map((m, index) => ({
      index,
      ...meta.resolveMember(m, type)
    }));
  }

  static getOrderMembers(state) {
    const { query, meta } = state;

    if (!meta) {
      return [];
    }

    const toOrderMember = (member) => ({
      id: member.name,
      title: member.title
    });

    return uniqBy(
      prop('id'),
      [
        ...QueryBuilder.resolveMember('measures', state).map(toOrderMember),
        ...QueryBuilder.resolveMember('dimensions', state).map(toOrderMember),
        ...QueryBuilder.resolveMember('timeDimensions', state).map((td) => toOrderMember(td.dimension))
      ].map((member) => ({
        ...member,
        order: (query.order?.[member.id]) || 'none'
      }))
    );
  }

  constructor(props) {
    super(props);

    this.state = {
      query: props.query,
      chartType: 'line',
      orderMembers: [],
      pivotConfig: null,
      validatedQuery: props.query,
      ...props.vizState
    };

    this.mutexObj = {};
  }

  async componentDidMount() {
    const { query, pivotConfig } = this.state;
    let meta;
    let pivotQuery;

    if (this.isQueryPresent()) {
      [meta, { pivotQuery }] = await Promise.all([
        this.cubejsApi().meta(),
        this.cubejsApi().dryRun(query)
      ]);
    } else {
      meta = await this.cubejsApi().meta();
    }

    this.setState({
      meta,
      orderMembers: QueryBuilder.getOrderMembers({ meta, query }),
      pivotConfig: ResultSet.getNormalizedPivotConfig(pivotQuery || {}, pivotConfig)
    });
  }

  cubejsApi() {
    const { cubejsApi } = this.props;
    // eslint-disable-next-line react/destructuring-assignment
    return cubejsApi || (this.context && this.context.cubejsApi);
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
        ...rangeSelection
      };
    };
    const toFilter = (member) => ({
      dimension: member.dimension.name,
      operator: member.operator,
      values: member.values
    });

    const updateMethods = (memberType, toQuery = getName) => ({
      add: (member) => {
        const { query } = this.state;
        this.updateQuery({
          [memberType]: (query[memberType] || []).concat(toQuery(member))
        });
      },
      remove: (member) => {
        const { query } = this.state;
        const members = (query[memberType] || []).concat([]);
        members.splice(member.index, 1);
        return this.updateQuery({
          [memberType]: members
        });
      },
      update: (member, updateWith) => {
        const { query } = this.state;
        const members = (query[memberType] || []).concat([]);
        members.splice(member.index, 1, toQuery(updateWith));
        return this.updateQuery({
          [memberType]: members
        });
      }
    });

    const {
      meta,
      query,
      orderMembers = [],
      chartType,
      pivotConfig,
      validatedQuery
    } = this.state;
    
    const flatFilters = uniqBy(
      prop('member'),
      flattenFilters((meta && query.filters) || []).map((filter) => ({
        ...filter,
        member: filter.member || filter.dimension
      }))
    );
    
    const filters = flatFilters.map((m, i) => ({
      ...m,
      dimension: meta.resolveMember(m.member || m.dimension, ['dimensions', 'measures']),
      operators: meta.filterOperatorsForMember(m.member || m.dimension, ['dimensions', 'measures']),
      index: i
    }));

    return {
      meta,
      query,
      validatedQuery,
      isQueryPresent: this.isQueryPresent(),
      chartType,
      measures: QueryBuilder.resolveMember('measures', this.state),
      dimensions: QueryBuilder.resolveMember('dimensions', this.state),
      timeDimensions: QueryBuilder.resolveMember('timeDimensions', this.state),
      segments: ((meta && query.segments) || []).map((m, i) => ({ index: i, ...meta.resolveMember(m, 'segments') })),
      filters,
      orderMembers,
      availableMeasures: (meta && meta.membersForQuery(query, 'measures')) || [],
      availableDimensions: (meta && meta.membersForQuery(query, 'dimensions')) || [],
      availableTimeDimensions: ((meta && meta.membersForQuery(query, 'dimensions')) || []).filter(
        (m) => m.type === 'time'
      ),
      availableSegments: (meta && meta.membersForQuery(query, 'segments')) || [],
      updateQuery: (queryUpdate) => this.updateQuery(queryUpdate),
      updateMeasures: updateMethods('measures'),
      updateDimensions: updateMethods('dimensions'),
      updateSegments: updateMethods('segments'),
      updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
      updateFilters: updateMethods('filters', toFilter),
      updateChartType: (newChartType) => this.updateVizState({ chartType: newChartType }),
      updateOrder: {
        set: (memberId, order = 'asc') => {
          this.updateVizState({
            orderMembers: orderMembers.map((orderMember) => ({
              ...orderMember,
              order: orderMember.id === memberId ? order : orderMember.order
            }))
          });
        },
        update: (order) => {
          this.updateQuery({
            order
          });
        },
        reorder: (sourceIndex, destinationIndex) => {
          if (sourceIndex == null || destinationIndex == null) {
            return;
          }

          this.updateVizState({
            orderMembers: moveItemInArray(orderMembers, sourceIndex, destinationIndex)
          });
        }
      },
      pivotConfig,
      updatePivotConfig: {
        moveItem: ({
          sourceIndex,
          destinationIndex,
          sourceAxis,
          destinationAxis,
        }) => {
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
            pivotConfig: nextPivotConfig
          });
        },
        update: (config) => {
          const { limit } = config;

          if (limit == null) {
            this.updateVizState({
              pivotConfig: {
                ...pivotConfig,
                ...config
              },
            });
          } else {
            this.updateQuery({ limit });
          }
        }
      },
      ...queryRendererProps
    };
  }

  updateQuery(queryUpdate) {
    const { query } = this.state;

    this.updateVizState({
      query: {
        ...query,
        ...queryUpdate
      }
    });
  }

  async updateVizState(state) {
    const { setQuery, setVizState } = this.props;
    const { query: stateQuery, pivotConfig: statePivotConfig } = this.state;

    let finalState = this.applyStateChangeHeuristics(state);
    const query = { ...(finalState.query || stateQuery) };

    const runSetters = (currentState) => {
      if (setVizState) {
        const { meta, validatedQuery, ...toSet } = currentState;
        setVizState(toSet);
      }
      if (currentState.query && setQuery) {
        setQuery(currentState.query);
      }
    };

    if (finalState.shouldApplyHeuristicOrder) {
      query.order = defaultOrder(query);
    }

    const updatedOrderMembers = indexBy(prop('id'), QueryBuilder.getOrderMembers({
      ...this.state,
      ...finalState
    }));
    const currentOrderMemberIds = (finalState.orderMembers || []).map(({ id }) => id);
    const currentOrderMembers = (finalState.orderMembers || []).filter(({ id }) => Boolean(updatedOrderMembers[id]));

    Object.entries(updatedOrderMembers).forEach(([id, orderMember]) => {
      if (!currentOrderMemberIds.includes(id)) {
        currentOrderMembers.push(orderMember);
      }
    });

    const nextQuery = {
      ...query,
      order: fromPairs(currentOrderMembers.map(({ id, order }) => (order !== 'none' ? [id, order] : false)).filter(Boolean))
    };

    finalState.pivotConfig = ResultSet.getNormalizedPivotConfig(
      nextQuery,
      finalState.pivotConfig !== undefined ? finalState.pivotConfig : statePivotConfig
    );

    runSetters({
      ...state,
      query: nextQuery,
      orderMembers: currentOrderMembers,
    });
    this.setState({
      ...finalState,
      query: nextQuery,
      orderMembers: currentOrderMembers,
    });

    let pivotQuery = {};
    if (QueryRenderer.isQueryPresent(query)) {
      try {
        const response = await this.cubejsApi().dryRun(query, {
          mutexObj: this.mutexObj,
        });
        pivotQuery = response.pivotQuery;

        if (finalState.shouldApplyHeuristicOrder) {
          nextQuery.order = (response.queryOrder || []).reduce((memo, current) => ({ ...memo, ...current }), {});
        }

        if (QueryRenderer.isQueryPresent(stateQuery)) {
          finalState = {
            ...finalState,
            query: nextQuery,
            pivotConfig: ResultSet.getNormalizedPivotConfig(pivotQuery, finalState.pivotConfig)
          };

          this.setState({
            ...finalState,
            validatedQuery: this.validatedQuery(finalState)
          });
          runSetters({
            ...this.state,
            ...finalState
          });
        }
      } catch (error) {
        console.error(error);
      }
    }
  }

  validatedQuery(state) {
    const { query } = state || this.state;

    return {
      ...query,
      filters: (query.filters || []).filter((f) => f.operator)
    };
  }

  defaultHeuristics(newState) {
    const { query, sessionGranularity } = this.state;
    const defaultGranularity = sessionGranularity || 'day';

    if (Array.isArray(query)) {
      return newState;
    }

    if (newState.query) {
      const oldQuery = query;
      let newQuery = newState.query;

      const { meta } = this.state;

      if (
        (oldQuery.timeDimensions || []).length === 1
        && (newQuery.timeDimensions || []).length === 1
        && newQuery.timeDimensions[0].granularity
        && oldQuery.timeDimensions[0].granularity !== newQuery.timeDimensions[0].granularity
      ) {
        newState = {
          ...newState,
          sessionGranularity: newQuery.timeDimensions[0].granularity
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
                granularity: defaultGranularity
              }
            ]
            : []
        };

        return {
          ...newState,
          pivotConfig: null,
          shouldApplyHeuristicOrder: true,
          query: newQuery,
          chartType: defaultTimeDimension ? 'line' : 'number'
        };
      }

      if ((oldQuery.dimensions || []).length === 0 && (newQuery.dimensions || []).length > 0) {
        newQuery = {
          ...newQuery,
          timeDimensions: (newQuery.timeDimensions || []).map((td) => ({ ...td, granularity: undefined }))
        };

        return {
          ...newState,
          pivotConfig: null,
          shouldApplyHeuristicOrder: true,
          query: newQuery,
          chartType: 'table'
        };
      }

      if ((oldQuery.dimensions || []).length > 0 && (newQuery.dimensions || []).length === 0) {
        newQuery = {
          ...newQuery,
          timeDimensions: (newQuery.timeDimensions || []).map((td) => ({
            ...td,
            granularity: td.granularity || defaultGranularity
          }))
        };

        return {
          ...newState,
          pivotConfig: null,
          shouldApplyHeuristicOrder: true,
          query: newQuery,
          chartType: (newQuery.timeDimensions || []).length ? 'line' : 'number'
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
          filters: []
        };

        return {
          ...newState,
          pivotConfig: null,
          shouldApplyHeuristicOrder: true,
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
          pivotConfig: null,
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
          pivotConfig: null,
          shouldApplyHeuristicOrder: true,
          query: {
            ...query,
            timeDimensions: [{ ...td, granularity: undefined }]
          }
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
    return (stateChangeHeuristics && stateChangeHeuristics(this.state, newState))
      || this.defaultHeuristics(newState);
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
  query: {},
  setQuery: null,
  setVizState: null,
  stateChangeHeuristics: null,
  disableHeuristics: false,
  render: null,
  wrapWithQueryRenderer: true,
  vizState: {}
};
