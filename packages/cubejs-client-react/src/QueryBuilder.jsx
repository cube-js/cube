import React from 'react';
import {
  prop, uniqBy, indexBy, fromPairs
} from 'ramda';
import { ResultSet, defaultHeuristics, moveItemInArray } from '@cubejs-client/core';
import QueryRenderer from './QueryRenderer.jsx';
import CubeContext from './CubeContext';

const granularities = [
  { name: undefined, title: 'w/o grouping' },
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
    const toTimeDimension = (member) => ({
      dimension: member.dimension.name,
      granularity: member.granularity,
      dateRange: member.dateRange
    });
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
      filters: ((meta && query.filters) || []).map((m, i) => ({
        ...m,
        dimension: meta.resolveMember(m.dimension, ['dimensions', 'measures']),
        operators: meta.filterOperatorsForMember(m.dimension, ['dimensions', 'measures']),
        index: i
      })),
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
    
    let pivotQuery = {};
    let finalState = this.applyStateChangeHeuristics(state);
    const { order: _, ...query } = finalState.query || stateQuery;
    
    const runSetters = (currentState) => {
      if (currentState.query && setQuery) {
        setQuery(currentState.query);
      }
      if (setVizState) {
        const { meta, validatedQuery, ...toSet } = currentState;
        setVizState(toSet);
      }
    };

    runSetters({
      ...state,
      query
    });
    this.setState({
      ...state,
      query
    });
    
    if (QueryRenderer.isQueryPresent(query)) {
      try {
        const response = await this.cubejsApi().dryRun(query, {
          mutexObj: this.mutexObj,
        });
        pivotQuery = response.pivotQuery;

        if (finalState.shouldApplyHeuristicOrder) {
          finalState.query.order = (response.queryOrder || []).reduce((memo, current) => ({ ...memo, ...current }), {});
        }
      } catch (error) {
        console.error(error);
      }
    }
    
    const activePivotConfig = finalState.pivotConfig !== undefined ? finalState.pivotConfig : statePivotConfig;
    
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
      
    const nextOrder = fromPairs(currentOrderMembers.map(({ id, order }) => (order !== 'none' ? [id, order] : false)).filter(Boolean));
    const nextQuery = {
      ...query,
      order: nextOrder,
    };

    finalState = {
      ...finalState,
      query: nextQuery,
      orderMembers: currentOrderMembers,
      pivotConfig: ResultSet.getNormalizedPivotConfig(pivotQuery, activePivotConfig)
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

  validatedQuery(state) {
    const { query } = state || this.state;

    return {
      ...query,
      filters: (query.filters || []).filter((f) => f.operator)
    };
  }

  applyStateChangeHeuristics(newState) {
    const { query, meta, sessionGranularity } = this.state;
    const { stateChangeHeuristics, disableHeuristics } = this.props;
    if (disableHeuristics) {
      return newState;
    }
    return (stateChangeHeuristics && stateChangeHeuristics(this.state, newState))
      || defaultHeuristics(newState.query, query, {
        meta,
        sessionGranularity
      });
  }

  render() {
    const { validatedQuery } = this.state;
    const { cubejsApi, render, wrapWithQueryRenderer } = this.props;
    
    if (wrapWithQueryRenderer) {
      return (
        <QueryRenderer
          query={validatedQuery}
          cubejsApi={cubejsApi}
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
