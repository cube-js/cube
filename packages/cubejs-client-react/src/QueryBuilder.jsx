import React from 'react';
import * as PropTypes from 'prop-types';
import {
  equals, prop, uniqBy, indexBy
} from 'ramda';
import QueryRenderer from './QueryRenderer.jsx';
import CubeContext from './CubeContext';
import { moveKeyAtIndex } from './utils';

const granularities = [
  { name: undefined, title: 'w/o grouping' },
  { name: 'hour', title: 'Hour' },
  { name: 'day', title: 'Day' },
  { name: 'week', title: 'Week' },
  { name: 'month', title: 'Month' },
  { name: 'year', title: 'Year' }
];

export default class QueryBuilder extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      query: props.query,
      chartType: 'line',
      orderMembers: [],
      ...props.vizState
    };

    this.shouldApplyHeuristicOrder = false;
    this.requestId = 0;
  }

  async componentDidMount() {
    const { query } = this.state;
    const meta = await this.cubejsApi().meta();

    const getInitialOrderMembers = () => {
      const indexById = Object.fromEntries(Object.keys(query.order || {}).map((id, index) => [id, index]));

      return this.getOrderMembers().sort((a, b) => {
        const a1 = indexById[a.id] === undefined ? Number.MAX_SAFE_INTEGER : indexById[a.id];
        const b1 = indexById[b.id] === undefined ? Number.MAX_SAFE_INTEGER : indexById[b.id];
        return a1 - b1;
      });
    };

    this.setState({ meta }, () => {
      this.setState({ orderMembers: getInitialOrderMembers() });
    });
  }

  componentDidUpdate(prevProps, prevState) {
    const { query, vizState } = this.props;

    if (!equals(prevProps.query, query)) {
      // eslint-disable-next-line react/no-did-update-set-state
      this.setState({ query });
    }

    if (!equals(prevProps.vizState, vizState)) {
      // eslint-disable-next-line react/no-did-update-set-state
      this.setState(vizState);
    }

    if (!equals(prevState.query, this.state.query)) {
      const { query, orderMembers } = this.state;

      const indexedOrderMembers = indexBy(prop('id'), this.getOrderMembers());
      const currentIndexedOrderMembers = orderMembers.map((m) => m.id);

      const nextOrderMembers = orderMembers
        .map((orderMember) => ({
          ...orderMember,
          order: (query.order && query.order[orderMember.id]) || 'none'
        }))
        .filter(({ id }) => Boolean(indexedOrderMembers[id]));

      Object.entries(indexedOrderMembers).forEach(([id, orderMember]) => {
        if (!currentIndexedOrderMembers.includes(id)) {
          nextOrderMembers.push(orderMember);
        }
      });

      const adjustedOrder = (query) => {
        const orderMembers = [
          ...(query.measures || []),
          ...(query.dimensions || []),
          ...(query.timeDimensions || []).map((td) => td.dimension)
        ];

        const order = Object.fromEntries(
          Object.entries(query.order || {})
            .map(([member, order]) => (orderMembers.includes(member) ? [member, order] : false))
            .filter(Boolean)
        );

        return (query.order == null && !Object.keys(order).length) || !orderMembers.length ? null : order;
      };

      const order = adjustedOrder(query);
      const { order: _, ...nextQuery } = query;

      this.updateVizState({
        query: {
          ...nextQuery,
          ...(order ? { order } : {})
        },
        orderMembers: nextOrderMembers
      });
    }
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

  getOrderMembers() {
    const { query } = this.state;

    const toOrderMember = (member) => ({
      id: member.name,
      title: member.title
    });

    return uniqBy(
      prop('id'),
      [
        ...this.resolveMember('measures').map(toOrderMember),
        ...this.resolveMember('dimensions').map(toOrderMember),
        ...this.resolveMember('timeDimensions').map((td) => toOrderMember(td.dimension))
      ].map((member) => ({
        ...member,
        order: (query.order && query.order[member.id]) || 'none'
      }))
    );
  }

  resolveMember(type) {
    const { meta, query } = this.state;

    if (!meta) {
      return [];
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

    const self = this;
    const {
      meta, query, orderMembers = [], chartType
    } = this.state;

    return {
      meta,
      query,
      validatedQuery: this.validatedQuery(),
      isQueryPresent: this.isQueryPresent(),
      chartType,
      measures: this.resolveMember('measures'),
      dimensions: this.resolveMember('dimensions'),
      timeDimensions: this.resolveMember('timeDimensions'),
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

      updateMeasures: updateMethods('measures'),
      updateDimensions: updateMethods('dimensions'),
      updateSegments: updateMethods('segments'),
      updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
      updateFilters: updateMethods('filters', toFilter),
      updateChartType: (newChartType) => this.updateVizState({ chartType: newChartType }),
      updateOrder: {
        set(member, order = 'asc', atIndex = null) {
          if (order === 'none') {
            this.remove(member);
          }

          let nextOrder;
          if (atIndex !== null) {
            nextOrder = moveKeyAtIndex(query.order, member, atIndex);
            nextOrder[member] = order;
          } else {
            nextOrder = {
              ...query.order,
              [member]: order
            };
          }

          self.updateQuery({ order: nextOrder });
        },
        remove: (member) => {
          this.updateQuery({
            order: Object.keys(query.order)
              .filter((currentMember) => currentMember !== member)
              .reduce((memo, currentMember) => {
                memo[currentMember] = query.order[currentMember];
                return memo;
              }, {})
          });
        },
        update: (order) => {
          this.updateQuery({
            order
          });
        },
        updateByOrderMembers: (orderMembers = []) => {
          this.setState({ orderMembers });
          this.updateQuery({
            order:
              Object.fromEntries(
                orderMembers.map(({ id, order }) => order !== 'none' && [id, order]).filter(Boolean)
              ) || {}
          });
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
    let finalState = this.applyStateChangeHeuristics(state);

    if (this.shouldApplyHeuristicOrder) {
      try {
        const currentRequestId = ++this.requestId;
        const { order: _, ...query } = finalState.query;
        const { sqlQuery } = await this.cubejsApi().sql(query);

        if (this.requestId === currentRequestId) {
          finalState = {
            ...finalState,
            query: {
              ...finalState.query,
              order: sqlQuery.sql.order
            }
          };
        }
      } catch (e) {}

      this.shouldApplyHeuristicOrder = false;
    }

    this.setState(finalState);
    finalState = { ...this.state, ...finalState };
    if (setQuery) {
      setQuery(finalState.query);
    }
    if (setVizState) {
      const { meta, ...toSet } = finalState;
      setVizState(toSet);
    }
  }

  validatedQuery() {
    const { query } = this.state;
    return {
      ...query,
      filters: (query.filters || []).filter((f) => f.operator)
    };
  }

  defaultHeuristics(newState) {
    const { query, sessionGranularity } = this.state;
    const defaultGranularity = sessionGranularity || 'day';

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

        this.shouldApplyHeuristicOrder = true;

        return {
          ...newState,
          query: newQuery,
          chartType: defaultTimeDimension ? 'line' : 'number'
        };
      }

      if ((oldQuery.dimensions || []).length === 0 && (newQuery.dimensions || []).length > 0) {
        newQuery = {
          ...newQuery,
          timeDimensions: (newQuery.timeDimensions || []).map((td) => ({ ...td, granularity: undefined }))
        };

        this.shouldApplyHeuristicOrder = true;

        return {
          ...newState,
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

        this.shouldApplyHeuristicOrder = true;

        return {
          ...newState,
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

        this.shouldApplyHeuristicOrder = true;

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

  applyStateChangeHeuristics(newState) {
    const { stateChangeHeuristics, disableHeuristics } = this.props;
    if (disableHeuristics) {
      return newState;
    }
    return (stateChangeHeuristics && stateChangeHeuristics(this.state, newState)) || this.defaultHeuristics(newState);
  }

  render() {
    const { cubejsApi, render, wrapWithQueryRenderer } = this.props;
    if (wrapWithQueryRenderer) {
      return (
        <QueryRenderer
          query={this.validatedQuery()}
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

QueryBuilder.propTypes = {
  render: PropTypes.func,
  stateChangeHeuristics: PropTypes.func,
  setQuery: PropTypes.func,
  setVizState: PropTypes.func,
  cubejsApi: PropTypes.object,
  disableHeuristics: PropTypes.bool,
  wrapWithQueryRenderer: PropTypes.bool,
  query: PropTypes.object,
  vizState: PropTypes.object
};

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
