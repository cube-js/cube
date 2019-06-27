import Vue from 'vue';
import QueryRenderer from './QueryRenderer';

export default Vue.component('QueryBuilder', {
  components: {
    QueryRenderer,
  },
  props: {
    query: {
      type: Object,
    },
    cubejsApi: {
      type: Object,
      required: true,
    },
  },
  data() {
    return {
      meta: undefined,
      internalQuery: this.query,
      granularities: [],
    };
  },
  async mounted() {
    this.meta = await this.cubejsApi.meta();

    this.granularities = [
      { name: 'hour', title: 'Hour' },
      { name: 'day', title: 'Day' },
      { name: 'week', title: 'Week' },
      { name: 'month', title: 'Month' },
      { name: 'year', title: 'Year' },
    ];
  },
  render(createElement) {
    const { cubejsApi, meta } = this;

    if (meta) {
      let toQuery = member => member.name;
      const queryElements = ['measures', 'dimensions', 'segments', 'timeDimensions', 'filters'];

      const childProps = {
        meta,
        query: this.internalQuery,
        validatedQuery: this.validatedQuery,
        isQueryPresent: this.isQueryPresent,
        chartType: this.chartType,
        measures: (this.internalQuery.measures || [])
          .map((m, i) => ({ index: i, ...meta.resolveMember(m, 'measures') })),
        dimensions: (this.internalQuery.dimensions || [])
          .map((m, i) => ({ index: i, ...meta.resolveMember(m, 'dimensions') })),
        segments: (this.internalQuery.segments || [])
          .map((m, i) => ({ index: i, ...meta.resolveMember(m, 'segments') })),
        timeDimensions: (this.internalQuery.timeDimensions || [])
          .map((m, i) => ({
            ...m,
            dimension: { ...meta.resolveMember(m.dimension, 'dimensions'), granularities: this.granularities },
            index: i
          })),
        filters: (this.internalQuery.filters || [])
          .map((m, i) => ({
            ...m,
            dimension: meta.resolveMember(m.dimension, ['dimensions', 'measures']),
            operators: meta.filterOperatorsForMember(m.dimension, ['dimensions', 'measures']),
            index: i
          })),
        availableMeasures: meta.membersForQuery(this.internalQuery, 'measures') || [],
        availableDimensions: meta.membersForQuery(this.internalQuery, 'dimensions') || [],
        availableTimeDimensions: (meta.membersForQuery(this.internalQuery, 'dimensions') || [])
          .filter(m => m.type === 'time'),
        availableSegments: meta.membersForQuery(this.internalQuery, 'segments') || [],
        updateChartType: this.updateChart,
      };

      queryElements.forEach((e) => {
        if (e === 'timeDimensions') {
          toQuery = (member) => ({
            dimension: member.dimension.name,
            granularity: member.granularity,
            dateRange: member.dateRange,
          });
        } else if (e === 'filters') {
          toQuery = (member) => ({
            dimension: member.dimension.name,
            operator: member.operator,
            values: member.values,
          });
        }

        const name = e.charAt(0).toUpperCase() + e.slice(1);

        childProps[`add${name}`] = (member) => {
          this.internalQuery = {
            ...this.internalQuery,
            [e]: (this.internalQuery[e] || []).concat(toQuery(member)),
          };
        };

        childProps[`update${name}`] = (member, updateWith) => {
          const members = (this.internalQuery[e] || []).concat([]);
          members.splice(member.index, 1, toQuery(updateWith));

          this.internalQuery = {
            ...this.internalQuery,
            [e]: members,
          };
        };

        childProps[`remove${name}`] = (member) => {
          const members = (this.internalQuery[e] || []).concat([]);
          members.splice(member.index, 1);

          this.internalQuery = {
            ...this.internalQuery,
            [e]: members,
          };
        };

        childProps[`set${name}`] = (members) => {
          this.internalQuery = {
            ...this.internalQuery,
            [e]: members.map(e => e.name) || [],
          };
        };
      });

      return createElement(QueryRenderer, {
        props: {
          query: this.validatedQuery,
          cubejsApi,
          builderProps: childProps,
        },
        scopedSlots: this.$scopedSlots,
      });
    } else {
      return null;
    }
  },
  computed: {
    isQueryPresent() {
      const { internalQuery: query } = this;

      return query.measures && query.measures.length  > 0 ||
        query.dimensions && query.dimensions.length > 0 ||
        query.timeDimensions && query.timeDimensions.length > 0;
    },
    validatedQuery() {
      const { internalQuery } = this;

      return {
        ...internalQuery,
        filters: (internalQuery.filters || []).filter(f => f.operator),
      };
    },
  },
  methods: {
    updateChart(chartType) {
      this.chartType = chartType;
    },
  },
});
