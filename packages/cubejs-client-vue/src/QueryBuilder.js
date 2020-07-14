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
      chartType: undefined,
      measures: [],
      dimensions: [],
      segments: [],
      timeDimensions: [],
      filters: [],
      availableMeasures: [],
      availableDimensions: [],
      availableTimeDimensions: [],
      availableSegments: [],
      limit: null,
      offset: null,
      renewQuery: false,
      order: null,
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
      limit,
      offset,
      setLimit,
      removeLimit,
      setOffset,
      removeOffset,
      renewQuery,
      order
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
        limit,
        offset,
        setLimit,
        removeLimit,
        setOffset,
        removeOffset,
        renewQuery,
        order,
        setOrder: this.setOrder
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
      // TODO: implement timezone

      let hasElements = false;
      QUERY_ELEMENTS.forEach((e) => {
        if (!this[e]) {
          return;
        }

        if (e === 'timeDimensions') {
          toQuery = (member) => ({
            dimension: member.dimension.name,
            granularity: member.granularity,
            dateRange: member.dateRange,
          });
        } else if (e === 'filters') {
          toQuery = (member) => ({
            member: member.member.name,
            operator: member.operator,
            values: member.values,
          });
        }

        if (this[e].length > 0) {
          validatedQuery[e] = this[e].map(x => toQuery(x));

          hasElements = true;
        }
      });
      // TODO: implement default heuristics

      if (validatedQuery.filters) {
        validatedQuery.filters = validatedQuery.filters.filter(f => f.operator);
      }

      // only set limit and offset if there are elements otherwise an invalid request with just limit/offset
      // gets sent when the component is first mounted, but before the actual query is constructed.
      if (hasElements) {
        if (this.limit) {
          validatedQuery.limit = this.limit;
        }

        if (this.offset) {
          validatedQuery.offset = this.offset;
        }

        if (this.order) {
          validatedQuery.order = this.order;
        }

        if (this.renewQuery) {
          validatedQuery.renewQuery = this.renewQuery;
        }
      }

      return validatedQuery;
    },
  },

  async mounted() {
    this.meta = await this.cubejsApi.meta();

    this.copyQueryFromProps();
  },

  methods: {
    copyQueryFromProps() {
      const { measures, dimensions, segments, timeDimensions, filters, limit, offset, renewQuery, order } = this.query;

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
        member: this.meta.resolveMember(m.member || m.dimension, ['dimensions', 'measures']),
        operators: this.meta.filterOperatorsForMember(m.member || m.dimension, ['dimensions', 'measures']),
        index: i
      }));

      this.availableMeasures = this.meta.membersForQuery({}, 'measures') || [];
      this.availableDimensions = this.meta.membersForQuery({}, 'dimensions') || [];
      this.availableTimeDimensions = (this.meta.membersForQuery({}, 'dimensions') || [])
        .filter(m => m.type === 'time');
      this.availableSegments = this.meta.membersForQuery({}, 'segments') || [];
      this.limit = (limit || null);
      this.offset = (offset || null);
      this.renewQuery = (renewQuery || false);
      this.order = (order || null);
    },
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
        const filterMember = {
          ...this.meta.resolveMember(member.member || member.dimension, ['dimensions', 'measures']),
        };

        mem = {
          ...member,
          member: filterMember,
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
        const filterMember = {
          ...this.meta.resolveMember(member.member || member.dimension, ['dimensions', 'measures']),
        };

        mem = {
          ...member,
          member: filterMember,
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
          const member = {
            ...this.meta.resolveMember(m.member || m.dimension, ['dimensions', 'measures']),
          };

          mem = {
            ...m,
            member,
          };
        } else {
          mem = this[`available${name}`].find(x => x.name === m);
        }

        if (mem) { elements.push(mem); }
      });

      this[element] = elements;
    },
    setLimit(limit) {
      this.limit = limit;
    },
    removeLimit() {
      this.limit = null;
    },
    setOffset(offset) {
      this.offset = offset;
    },
    removeOffset() {
      this.offset = null;
    },
    updateChart(chartType) {
      this.chartType = chartType;
    },
    setOrder(order = {}) {
      this.order = order;
    }
  },

  watch: {
    query: {
      deep: true,
      handler() {
        if (!this.meta) {
          // this is ok as if meta has not been loaded by the time query prop has changed,
          // then the promise for loading meta (found in mounted()) will call
          // copyQueryFromProps and will therefore update anyway.
          return;
        }
        this.copyQueryFromProps();
      }
    }
  }
};
