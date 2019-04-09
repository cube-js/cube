import React from 'react';
import * as PropTypes from 'prop-types';
import QueryRenderer from './QueryRenderer.jsx';

export default class QueryBuilder extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      query: props.query,
      chartType: 'line'
    };
  }

  async componentDidMount() {
    const { cubejsApi } = this.props;
    const meta = await cubejsApi.meta();
    this.setState({ meta });
  }

  isQueryPresent() {
    const { query } = this.state;
    return query.measures && query.measures.length
      || query.dimensions && query.dimensions.length
      || query.timeDimensions && query.timeDimensions.length;
  }

  prepareRenderProps(queryRendererProps) {
    const getName = member => member.name;
    const toTimeDimension = member => ({
      dimension: member.dimension.name,
      granularity: member.granularity,
      dateRange: member.dateRange
    });
    const toFilter = member => ({
      dimension: member.dimension.name,
      operator: member.operator,
      values: member.values
    });
    const updateMethods = (memberType, toQuery = getName) => ({
      add: (member) => {
        const { query } = this.state;
        this.setState(this.applyStateChangeHeuristics({
          query: {
            ...query,
            [memberType]: (query[memberType] || []).concat(toQuery(member))
          }
        }));
      },
      remove: (member) => {
        const { query } = this.state;
        const members = (query[memberType] || []).concat([]);
        members.splice(member.index, 1);
        return this.setState(this.applyStateChangeHeuristics({
          query: {
            ...query,
            [memberType]: members
          }
        }));
      },
      update: (member, updateWith) => {
        const { query } = this.state;
        const members = (query[memberType] || []).concat([]);
        members.splice(member.index, 1, toQuery(updateWith));
        return this.setState(this.applyStateChangeHeuristics({
          query: {
            ...query,
            [memberType]: members
          }
        }));
      }
    });

    const granularities = [
      { name: undefined, title: 'w/o grouping' },
      { name: 'hour', title: 'Hour' },
      { name: 'day', title: 'Day' },
      { name: 'week', title: 'Week' },
      { name: 'month', title: 'Month' },
      { name: 'year', title: 'Year' }
    ];

    const { meta, query, chartType } = this.state;

    return {
      meta,
      query,
      validatedQuery: this.validatedQuery(),
      isQueryPresent: this.isQueryPresent(),
      chartType,
      measures: (meta && query.measures || [])
        .map((m, i) => ({ index: i, ...meta.resolveMember(m, 'measures') })),
      dimensions: (meta && query.dimensions || [])
        .map((m, i) => ({ index: i, ...meta.resolveMember(m, 'dimensions') })),
      segments: (meta && query.segments || [])
        .map((m, i) => ({ index: i, ...meta.resolveMember(m, 'segments') })),
      timeDimensions: (meta && query.timeDimensions || [])
        .map((m, i) => ({
          ...m,
          dimension: { ...meta.resolveMember(m.dimension, 'dimensions'), granularities },
          index: i
        })),
      filters: (meta && query.filters || [])
        .map((m, i) => ({
          ...m,
          dimension: meta.resolveMember(m.dimension, ['dimensions', 'measures']),
          operators: meta.filterOperatorsForMember(m.dimension, ['dimensions', 'measures']),
          index: i
        })),
      availableMeasures: meta && meta.membersForQuery(query, 'measures') || [],
      availableDimensions: meta && meta.membersForQuery(query, 'dimensions') || [],
      availableTimeDimensions: (
        meta && meta.membersForQuery(query, 'dimensions') || []
      ).filter(m => m.type === 'time'),
      availableSegments: meta && meta.membersForQuery(query, 'segments') || [],

      updateMeasures: updateMethods('measures'),
      updateDimensions: updateMethods('dimensions'),
      updateSegments: updateMethods('segments'),
      updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
      updateFilters: updateMethods('filters', toFilter),
      updateChartType: (newChartType) => this.setState(this.applyStateChangeHeuristics({ chartType: newChartType })),
      ...queryRendererProps
    };
  }

  validatedQuery() {
    const { query } = this.state;
    return {
      ...query,
      filters: (query.filters || []).filter(f => f.operator)
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
        (oldQuery.measures || []).length === 0 && (newQuery.measures || []).length > 0
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
            granularity: defaultGranularity
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

  applyStateChangeHeuristics(newState) {
    const { stateChangeHeuristics, disableHeuristics } = this.props;
    if (disableHeuristics) {
      return newState;
    }
    return stateChangeHeuristics && stateChangeHeuristics(this.state, newState)
      || this.defaultHeuristics(newState);
  }

  render() {
    const { cubejsApi, render } = this.props;
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
  }
}

QueryBuilder.propTypes = {
  render: PropTypes.func,
  stateChangeHeuristics: PropTypes.func,
  cubejsApi: PropTypes.object.isRequired,
  disableHeuristics: PropTypes.bool,
  query: PropTypes.object
};

QueryBuilder.defaultProps = {
  query: {},
  stateChangeHeuristics: null,
  disableHeuristics: false,
  render: null
};
