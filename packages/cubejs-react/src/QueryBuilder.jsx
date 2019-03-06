import React from 'react';
import QueryRenderer from './QueryRenderer.jsx'

export default class QueryBuilder extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      query: props.query,
      chartType: 'line'
    };
  }

  async componentDidMount() {
    const meta = await this.props.cubejsApi.meta();
    this.setState({ meta });
  }

  render() {
    return (<QueryRenderer
      query={this.state.query}
      cubejsApi={this.props.cubejsApi}
      render={(queryRendererProps) => {
        if (this.props.render) {
          return this.props.render(this.prepareRenderProps(queryRendererProps));
        }
      }}
    />);
  };

  prepareRenderProps(queryRendererProps) {
    const getName = member => member.name;
    const toTimeDimension = member =>
      ({ dimension: member.dimension.name, granularity: member.granularity, dateRange: member.dateRange });
    const updateMethods = (memberType, toQuery = getName) => ({
      add: (member) => this.setState({
        query: {
          ...this.state.query,
          [memberType]: (this.state.query[memberType] || []).concat(toQuery(member))
        }
      }),
      remove: (member) => {
        const members = (this.state.query[memberType] || []).concat([]);
        members.splice(member.index, 1);
        return this.setState({
          query: {
            ...this.state.query,
            [memberType]: members
          }
        })
      },
      update: (member, updateWith) => {
        const members = (this.state.query[memberType] || []).concat([]);
        members.splice(member.index, 1, toQuery(updateWith));
        return this.setState({
          query: {
            ...this.state.query,
            [memberType]: members
          }
        })
      }
    });

    const granularities = [
      { name: 'hour', title: 'Hour' },
      { name: 'day', title: 'Day' },
      { name: 'week', title: 'Week' },
      { name: 'month', title: 'Month' },
      { name: 'year', title: 'Year' }
    ];

    return {
      meta: this.state.meta,
      query: this.state.query,
      chartType: this.state.chartType,
      measures: (this.state.meta && this.state.query.measures || [])
        .map((m, i) => ({ index: i, ...this.state.meta.resolveMember(m, 'measures') })),
      dimensions: (this.state.meta && this.state.query.dimensions || [])
        .map((m, i) => ({ index: i, ...this.state.meta.resolveMember(m, 'dimensions') })),
      segments: (this.state.meta && this.state.query.segments || [])
        .map((m, i) => ({ index: i, ...this.state.meta.resolveMember(m, 'segments') })),
      timeDimensions: (this.state.meta && this.state.query.timeDimensions || [])
        .map((m, i) => ({
          ...m,
          dimension: { ...this.state.meta.resolveMember(m.dimension, 'dimensions'), granularities },
          index: i
        })),
      filters: (this.state.meta && this.state.query.filters || [])
        .map((m, i) => ({ ...m, dimension: this.state.meta.resolveMember(m.dimension, 'dimensions'), index: i })),
      availableMeasures: this.state.meta && this.state.meta.membersForQuery(this.state.query, 'measures') || [],
      availableDimensions: this.state.meta && this.state.meta.membersForQuery(this.state.query, 'dimensions') || [],
      availableTimeDimensions: (
        this.state.meta && this.state.meta.membersForQuery(this.state.query, 'dimensions') || []
      ).filter(m => m.type === 'time'),
      availableSegments: this.state.meta && this.state.meta.membersForQuery(this.state.query, 'segments') || [],

      updateMeasures: updateMethods('measures'),
      updateDimensions: updateMethods('dimensions'),
      updateSegments: updateMethods('segments'),
      updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
      updateChartType: (chartType) => this.setState({ chartType }),
      ...queryRendererProps
    };
  }
}