import {
  isQueryPresent,
  defaultOrder,
  defaultHeuristics,
  GRANULARITIES,
  ResultSet,
  getOrderMembersFromOrder,
  moveItemInArray,
} from '@cubejs-client/core';
import { equals, fromPairs } from 'ramda';

import QueryRenderer from './QueryRenderer';

const QUERY_ELEMENTS = ['measures', 'dimensions', 'segments', 'timeDimensions', 'filters'];

// todo: remove
const d = (v) => JSON.parse(JSON.stringify(v));

const toOrderMember = (member) => ({
  id: member.name,
  title: member.title,
});

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
    disableHeuristics: {
      type: Boolean,
    },
    stateChangeHeuristics: {
      type: Function,
    },
  },
  data() {
    return {
      skipHeuristics: true,
      meta: undefined,
      mutex: {},
      chartType: 'line',
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
      orderMembers: [],
      prevValidatedQuery: null,
      granularities: GRANULARITIES,
      pivotConfig: ResultSet.getNormalizedPivotConfig(this.query),
    };
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
      order,
      orderMembers,
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
        orderMembers,
        setOrder: this.setOrder,
        setQuery: this.setQuery,
        pivotConfig: this.pivotConfig,
        updateOrder: {
          set: (memberId, order) => {
            this.orderMembers = this.orderMembers.map((orderMember) => ({
              ...orderMember,
              order: orderMember.id === memberId ? order : orderMember.order,
            }));
          },
          update: (newOrder) => {
            this.order = newOrder;
          },
          reorder: (sourceIndex, destinationIndex) => {
            this.orderMembers = moveItemInArray(this.orderMembers, sourceIndex, destinationIndex);
          },
        },
      };

      QUERY_ELEMENTS.forEach((elementName) => {
        const name = elementName.charAt(0).toUpperCase() + elementName.slice(1);

        builderProps[`add${name}`] = (member) => {
          this.addMember(elementName, member);
        };

        builderProps[`update${name}`] = (member, updateWith) => {
          this.updateMember(elementName, member, updateWith);
        };

        builderProps[`remove${name}`] = (member) => {
          this.removeMember(elementName, member);
        };

        builderProps[`set${name}`] = (members) => {
          this.setMembers(elementName, members);
        };
      });
    }

    // Pass parent slots to child QueryRenderer component
    const children = Object.keys(this.$slots).map((slot) =>
      createElement('template', { slot }, this.$slots[slot])
    );

    return createElement(
      QueryRenderer,
      {
        props: {
          query: this.validatedQuery,
          cubejsApi,
          builderProps,
        },
        scopedSlots: this.$scopedSlots,
      },
      children
    );
  },
  computed: {
    isQueryPresent() {
      const { validatedQuery } = this;

      return isQueryPresent(validatedQuery);
    },
    validatedQuery() {
      let validatedQuery = {};
      let toQuery = (member) => member.name;
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
          validatedQuery[e] = this[e].map((x) => toQuery(x));

          hasElements = true;
        }
      });

      if (validatedQuery.filters) {
        validatedQuery.filters = validatedQuery.filters.filter((f) => f.operator);
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

      if (
        !this.skipHeuristics &&
        !this.disableHeuristics &&
        isQueryPresent(validatedQuery) &&
        this.meta
      ) {
        const heuristicsFn = this.stateChangeHeuristics || defaultHeuristics;
        const { query, chartType, shouldApplyHeuristicOrder, pivotConfig } = heuristicsFn(
          validatedQuery,
          this.prevValidatedQuery,
          {
            meta: this.meta,
            sessionGranularity: validatedQuery?.timeDimensions?.[0]?.granularity,
          }
        );

        validatedQuery = {
          ...validatedQuery,
          ...query,
          ...(shouldApplyHeuristicOrder ? { order: defaultOrder(query) } : null),
        };

        this.chartType = chartType || this.chartType;
        this.pivotConfig = ResultSet.getNormalizedPivotConfig(
          validatedQuery,
          pivotConfig || this.pivotConfig
        );
        this.copyQueryFromProps(validatedQuery);
      }

      this.prevValidatedQuery = validatedQuery;
      return validatedQuery;
    },
  },

  updated() {
    // query heuristics should only apply on query change (not applied to the initial query)
    this.skipHeuristics = false;
  },

  async mounted() {
    this.meta = await this.cubejsApi.meta();

    this.copyQueryFromProps();
    this.orderMembers = this.getOrderMembers();
  },

  methods: {
    getQuery() {
      return this.validatedQuery;
    },
    copyQueryFromProps(query) {
      const {
        measures = [],
        dimensions = [],
        segments = [],
        timeDimensions = [],
        filters = [],
        limit,
        offset,
        renewQuery,
        order,
      } = query || this.query;

      this.measures = measures.map((m, index) => ({
        index,
        ...this.meta.resolveMember(m, 'measures'),
      }));
      this.dimensions = dimensions.map((m, index) => ({
        index,
        ...this.meta.resolveMember(m, 'dimensions'),
      }));
      this.segments = segments.map((m, index) => ({
        index,
        ...this.meta.resolveMember(m, 'segments'),
      }));
      this.timeDimensions = timeDimensions.map((m, index) => ({
        ...m,
        dimension: {
          ...this.meta.resolveMember(m.dimension, 'dimensions'),
          granularities: this.granularities,
        },
        index,
      }));
      this.filters = filters.map((m, index) => ({
        ...m,
        member: this.meta.resolveMember(m.member || m.dimension, ['dimensions', 'measures']),
        operators: this.meta.filterOperatorsForMember(m.member || m.dimension, [
          'dimensions',
          'measures',
        ]),
        index,
      }));

      this.availableMeasures = this.meta.membersForQuery({}, 'measures') || [];
      this.availableDimensions = this.meta.membersForQuery({}, 'dimensions') || [];
      this.availableTimeDimensions = (this.meta.membersForQuery({}, 'dimensions') || []).filter(
        (m) => m.type === 'time'
      );
      this.availableSegments = this.meta.membersForQuery({}, 'segments') || [];
      this.limit = limit || 10000;
      this.offset = offset || null;
      this.renewQuery = renewQuery || false;
      this.order = order || null;
    },
    addMember(element, member) {
      const name = element.charAt(0).toUpperCase() + element.slice(1);
      let mem;

      if (element === 'timeDimensions') {
        mem = this[`available${name}`].find((m) => m.name === member.dimension);
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
        mem = this[`available${name}`].find((m) => m.name === member);
      }

      if (mem) {
        this[element].push(mem);
      }
    },
    removeMember(element, member) {
      const name = element.charAt(0).toUpperCase() + element.slice(1);
      let mem;

      if (element === 'timeDimensions') {
        mem = this[`available${name}`].find((x) => x.name === member);
      } else if (element === 'filters') {
        mem = member;
      } else {
        mem = this[`available${name}`].find((m) => m.name === member);
      }

      if (mem) {
        const index = this[element].findIndex((x) => x.name === mem);
        this[element].splice(index, 1);
      }
    },
    updateMember(element, old, member) {
      const name = element.charAt(0).toUpperCase() + element.slice(1);
      let mem;
      let index;

      if (element === 'timeDimensions') {
        index = this[element].findIndex((x) => x.dimension.name === old.dimension);
        mem = this[`available${name}`].find((m) => m.name === member.dimension);
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
        index = this[element].findIndex((x) => x.dimension === old);
        const filterMember = {
          ...this.meta.resolveMember(member.member || member.dimension, ['dimensions', 'measures']),
        };

        mem = {
          ...member,
          member: filterMember,
        };
      } else {
        index = this[element].findIndex((x) => x.name === old);
        mem = this[`available${name}`].find((m) => m.name === member);
      }

      if (mem) {
        this[element].splice(index, 1, mem);
      }
    },
    setMembers(element, members) {
      const name = element.charAt(0).toUpperCase() + element.slice(1);
      let mem;
      const elements = [];

      members.filter(Boolean).forEach((m) => {
        if (element === 'timeDimensions') {
          mem = this[`available${name}`].find((x) => x.name === m.dimension);
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
          mem = this[`available${name}`].find((x) => x.name === m);
        }

        if (mem) {
          elements.push(mem);
        }
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
    // todo: accept `order` as array of arrays
    setOrder(order = {}) {
      this.order = order;
    },
    getOrderMembers() {
      return [
        ...this.measures,
        ...this.dimensions,
        ...this.timeDimensions.map(({ dimension }) => toOrderMember(dimension)),
      ]
        .map((member, index) => {
          const id = member.name || member.id;

          if (!id) {
            return false;
          }

          return {
            index,
            id,
            title: member.title,
            order: this.order?.[id] || 'none',
          };
        })
        .filter(Boolean);
    },
  },

  watch: {
    validatedQuery: {
      deep: true,
      handler(query, prevQuery) {
        if (isQueryPresent(query) && !equals(query, prevQuery)) {
          this.cubejsApi
            .dryRun(query, {
              mutexObj: this.mutex,
            })
            .then(({ pivotQuery }) => {
              const pivotConfig = ResultSet.getNormalizedPivotConfig(pivotQuery, this.pivotConfig);

              if (!equals(pivotConfig, this.pivotConfig)) {
                this.pivotConfig = pivotConfig;
              }
            })
            .catch((error) => console.error(error));
        }
      },
    },
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
      },
    },
    order: {
      deep: true,
      handler(order) {
        const nextOrderMembers = getOrderMembersFromOrder(this.getOrderMembers(), order);

        // debugger;
        console.log('>>', nextOrderMembers, this.getOrderMembers());

        if (!equals(nextOrderMembers, this.getOrderMembers())) {
          this.orderMembers = nextOrderMembers;
        }
      },
    },
    orderMembers: {
      deep: true,
      handler(orderMembers) {
        const nextOrder = orderMembers
          .map(({ id, order }) => (order !== 'none' ? [id, order] : false))
          .filter(Boolean);
        if (!equals(Object.entries(this.order || {}), nextOrder)) {
          this.order = fromPairs(nextOrder);
        }
      },
    },
  },
};
