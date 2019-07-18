import QueryRenderer from './QueryRenderer';

const QUERY_ELEMENTS = ['measures', 'dimensions', 'segments', 'timeDimensions', 'filters'];

export default {
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
    const data = {
      meta: undefined,
      measures: [],
      dimensions: [],
      segments: [],
      timeDimensions: [],
      filters: [],
      availableMeasures: [],
      availableDimensions: [],
      availableTimeDimensions: [],
      availableSegments: [],
    };

    data.granularities = [
      { name: 'hour', title: 'Hour' },
      { name: 'day', title: 'Day' },
      { name: 'week', title: 'Week' },
      { name: 'month', title: 'Month' },
      { name: 'year', title: 'Year' },
    ];

    return data;
  },
  async mounted() {
    this.meta = await this.cubejsApi.meta();

    const { measures, dimensions, segments, timeDimensions, filters } = this.query;

    this.measures = (measures || []).map((m, i) => ({ index: i, ...this.meta.resolveMember(m, 'measures') }));
    this.dimensions = (dimensions || []).map((m, i) => ({ index: i, ...this.meta.resolveMember(m, 'dimensions') }));
    this.segments = (segments || []).map((m, i) => ({ index: i, ...this.meta.resolveMember(m, 'segments') }));
    this.timeDimensions = (timeDimensions || []).map((m, i) => ({
      ...m,
      dimension: { ...this.meta.resolveMember(m.dimension, 'dimensions'), granularities: this.granularities },
      index: i
    }));
    this.filters = (filters || []).map((m, i) => ({
      ...m,
      dimension: this.meta.resolveMember(m.dimension, ['dimensions', 'measures']),
      operators: this.meta.filterOperatorsForMember(m.dimension, ['dimensions', 'measures']),
      index: i
    }));

    this.availableMeasures = this.meta.membersForQuery({}, 'measures') || [];
    this.availableDimensions = this.meta.membersForQuery({}, 'dimensions') || [];
    this.availableTimeDimensions = (this.meta.membersForQuery({}, 'dimensions') || [])
      .filter(m => m.type === 'time');
    this.availableSegments = this.meta.membersForQuery({}, 'segments') || [];
  },
  render(createElement) {
    const {
      chartType,
      cubejsApi,
      dimensions,
      filters,
      measures,
      meta,
      query,
      segments,
      timeDimensions,
      validatedQuery,
      isQueryPresent,
      availableSegments,
      availableTimeDimensions,
      availableDimensions,
      availableMeasures,
    } = this;

    let builderProps = {};

    if (meta) {
      builderProps = {
        query,
        validatedQuery,
        isQueryPresent,
        chartType,
        measures,
        dimensions,
        segments,
        timeDimensions,
        filters,
        availableSegments,
        availableTimeDimensions,
        availableDimensions,
        availableMeasures,
        updateChartType: this.updateChart,
      };

      QUERY_ELEMENTS.forEach((e) => {
        const name = e.charAt(0).toUpperCase() + e.slice(1);

        builderProps[`add${name}`] = (member) => {
          this.addMember(e, member);
        };

        builderProps[`update${name}`] = (member, updateWith) => {
          this.updateMember(e, member, updateWith);
        };

        builderProps[`remove${name}`] = (member) => {
          this.removeMember(e, member);
        };

        builderProps[`set${name}`] = (members) => {
          this.setMembers(e, members);
        };
      });
    }

    // Pass parent slots to child QueryRenderer component
    const children = Object.keys(this.$slots).map(slot =>
      createElement('template', { slot }, this.$slots[slot]));

    return createElement(QueryRenderer, {
      props: {
        query: this.validatedQuery,
        cubejsApi,
        builderProps,
      },
      scopedSlots: this.$scopedSlots,
    }, children);
  },
  computed: {
    isQueryPresent() {
      const { query } = this;

      return Object.keys(query).length > 0;
    },
    validatedQuery() {
      const validatedQuery = {};
      let toQuery = member => member.name;
      // TODO: implement order, limit, timezone, renewQuery

      QUERY_ELEMENTS.forEach((e) => {
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

        if (this[e].length > 0) {
          validatedQuery[e] = this[e].map(x => toQuery(x));
        }
      });
      // TODO: implement default heuristics

      if (validatedQuery.filters) {
        validatedQuery.filters = validatedQuery.filters.filter(f => f.operator);
      }

      return validatedQuery;
    },
  },
  methods: {
    addMember(element, member) {
      const name = element.charAt(0).toUpperCase() + element.slice(1);
      let mem;

      if (element === 'timeDimensions') {
        mem = this[`available${name}`].find(m => m.name === member.dimension);
        if (mem) {
          const dimension = {
            ...this.meta.resolveMember(mem.name, 'dimensions'),
            granularities: this.granularities,
          };

          mem = {
            ...mem,
            granularity: member.granularity,
            dateRange: member.dateRange,
            dimension,
            index: this[element].length,
          };
        }
      } else if (element === 'filters') {
        const dimension = {
          ...this.meta.resolveMember(member.dimension, 'dimensions'),
        };

        mem = {
          ...member,
          dimension,
        };
      } else {
        mem = this[`available${name}`].find(m => m.name === member);
      }

      if (mem) { this[element].push(mem); }
    },
    removeMember(element, member) {
      const name = element.charAt(0).toUpperCase() + element.slice(1);
      let mem;

      if (element === 'timeDimensions') {
        mem = this[`available${name}`].find(x => x.name === member);
      } else if (element === 'filters') {
        mem = member;
      } else {
        mem = this[`available${name}`].find(m => m.name === member);
      }

      if (mem) {
        const index = this[element].findIndex(x => x.name === mem);
        this[element].splice(index, 1);
      }
    },
    updateMember(element, old, member) {
      const name = element.charAt(0).toUpperCase() + element.slice(1);
      let mem;
      let index;

      if (element === 'timeDimensions') {
        index = this[element].findIndex(x => x.dimension.name === old.dimension);
        mem = this[`available${name}`].find(m => m.name === member.dimension);
        if (mem) {
          const dimension = {
            ...this.meta.resolveMember(mem.name, 'dimensions'),
            granularities: this.granularities,
          };

          mem = {
            ...mem,
            dimension,
            granularity: member.granularity,
            dateRange: member.dateRange,
            index,
          };
        }
      } else if (element === 'filters') {
        index = this[element].findIndex(x => x.dimension === old);
        const dimension = {
          ...this.meta.resolveMember(member.dimension, 'dimensions'),
        };

        mem = {
          ...member,
          dimension,
        };
      } else {
        index = this[element].findIndex(x => x.name === old);
        mem = this[`available${name}`].find(m => m.name === member);
      }

      if (mem) {
        this[element].splice(index, 1, mem);
      }
    },
    setMembers(element, members) {
      const name = element.charAt(0).toUpperCase() + element.slice(1);
      let mem;
      const elements = [];

      members.forEach((m) => {
        if (element === 'timeDimensions') {
          mem = this[`available${name}`].find(x => x.name === m.dimension);
          if (mem) {
            const dimension = {
              ...this.meta.resolveMember(mem.name, 'dimensions'),
              granularities: this.granularities,
            };

            mem = {
              ...mem,
              granularity: m.granularity,
              dateRange: m.dateRange,
              dimension,
              index: this[element].length,
            };
          }
        } else if (element === 'filters') {
          const dimension = {
            ...this.meta.resolveMember(m.dimension, 'dimensions'),
          };

          mem = {
            ...m,
            dimension,
          };
        } else {
          mem = this[`available${name}`].find(x => x.name === m);
        }

        if (mem) { elements.push(mem); }
      });

      this[element] = elements;
    },
    updateChart(chartType) {
      this.chartType = chartType;
    },
  },
};
