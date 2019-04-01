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
  async mounted() {
    this.meta = await this.cubejsApi.meta();
  },
  render(createElement) {
    const { cubejsApi } = this;

    return createElement(QueryRenderer, {
      props: {
        query: this.validatedQuery(),
        cubejsApi,
      },
      // TODO: check passable props
    }, this.$scopedSlots.default(this.prepareRenderProps()));
  },
  methods: {
    isQueryPresent() {
      const { query } = this;

      return query.measures && query.measures.length ||
        query.dimensions && query.dimensions.length ||
        query.timeDimensions && query.timeDimensions.length;
    },
    validatedQuery() {
      const { query } = this;

      return {
        ...query,
        filters: (query.filters || []).filter(f => f.operator),
      };
    },
    prepareRenderProps(queryRendererProps) {
      const getName = member => member.name;
      const toTimeDimension = member => ({
        dimension: member.dimension.name,
        granularity: member.granularity,
        dateRange: member.dateRange,
      });
      const toFilter = member => ({
        dimension: member.dimension.name,
        operator: member.operator,
        values: member.values,
      });

      const updateMethods = (memberType, toQuery = getName) => ({
        add(member) {
          this.query = {
            ...this.query,
            [memberType]: (this.query[memberType] || []).concat(toQuery(member)),
          };
        },
        remove(member) {
          const members = (this.query[memberType] || []).concat([]);
          members.splice(member.index, 1);

          // TODO: check return state
          this.query = {
            ...this.query,
            [memberType]: members,
          };
        },
        update(member, updateWith) {
          const members = (this.query[memberType] || []).concat([]);
          members.splice(member.index, 1, toQuery(updateWith));

          // TODO: check return state
          this.query = {
            ...this.query,
            [memberType]: members,
          };
        },
      });

      const granularities = [
        { name: 'hour', title: 'Hour' },
        { name: 'day', title: 'Day' },
        { name: 'week', title: 'Week' },
        { name: 'month', title: 'Month' },
        { name: 'year', title: 'Year' },
      ];

      return {
        meta: this.meta,
        query: this.query,
        validatedQuery: this.validatedQuery(),
        isQueryPresent: this.isQueryPresent(),
        chartType: this.chartType,
        measures: (this.meta && this.query.measures || [])
          .map((m, i) => ({ index: i, ...this.meta.resolveMember(m, 'measures') })),
        dimensions: (this.meta && this.query.dimensions || [])
          .map((m, i) => ({ index: i, ...this.meta.resolveMember(m, 'dimensions') })),
        segments: (this.meta && this.query.segments || [])
          .map((m, i) => ({ index: i, ...this.meta.resolveMember(m, 'segments') })),
        timeDimensions: (this.meta && this.query.timeDimensions || [])
          .map((m, i) => ({
            ...m,
            dimension: { ...this.meta.resolveMember(m.dimension, 'dimensions'), granularities },
            index: i
          })),
        filters: (this.meta && this.query.filters || [])
          .map((m, i) => ({
            ...m,
            dimension: this.meta.resolveMember(m.dimension, ['dimensions', 'measures']),
            operators: this.meta.filterOperatorsForMember(m.dimension, ['dimensions', 'measures']),
            index: i
          })),
        availableMeasures: this.meta && this.meta.membersForQuery(this.query, 'measures') || [],
        availableDimensions: this.meta && this.meta.membersForQuery(this.query, 'dimensions') || [],
        availableTimeDimensions: (
          this.meta && this.meta.membersForQuery(this.query, 'dimensions') || []
        ).filter(m => m.type === 'time'),
        availableSegments: this.meta && this.meta.membersForQuery(this.query, 'segments') || [],

        updateMeasures: updateMethods('measures'),
        updateDimensions: updateMethods('dimensions'),
        updateSegments: updateMethods('segments'),
        updateTimeDimensions: updateMethods('timeDimensions', toTimeDimension),
        updateFilters: updateMethods('filters', toFilter),
        updateChartType: (chartType) => { this.chartType = chartType; },
        ...queryRendererProps,
      };
    },
  },
});
