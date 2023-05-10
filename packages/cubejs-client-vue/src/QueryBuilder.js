import {
  isQueryPresent,
  defaultOrder,
  defaultHeuristics,
  GRANULARITIES,
  ResultSet,
  getOrderMembersFromOrder,
  moveItemInArray,
  movePivotItem,
  areQueriesEqual,
} from '@cubejs-client/core';
import { clone, equals } from 'ramda';

import QueryRenderer from './QueryRenderer';

const QUERY_ELEMENTS = ['measures', 'dimensions', 'segments', 'timeDimensions', 'filters'];

const toOrderMember = (member) => ({
  id: member.name,
  title: member.title,
});

const reduceOrderMembers = (array) =>
  array.reduce((acc, { id, order }) => (order !== 'none' ? [...acc, [id, order]] : acc), []);

const operators = ['and', 'or'];

const validateFilters = (filters) =>
  filters.reduce((acc, raw) => {
    if (raw.operator) {
      return [...acc, raw];
    }

    const validBooleanFilter = operators.reduce((acc, operator) => {
      const filters = raw[operator];

      const booleanFilters = validateFilters(filters || []);

      if (booleanFilters.length) {
        return { ...acc, [operator]: booleanFilters };
      }

      return acc;
    }, {});

    if (operators.some((operator) => validBooleanFilter[operator])) {
      return [...acc, validBooleanFilter];
    }

    return acc;
  }, []);

const getDimensionOrMeasure = (meta, m) => {
  const memberName = m.member || m.dimension;
  return memberName && meta.resolveMember(memberName, ['dimensions', 'measures']);
};

const resolveMembers = (meta, arr) =>
  arr &&
  arr.map((e, index) => {
    return {
      ...e,
      member: getDimensionOrMeasure(meta, e),
      index,
      and: resolveMembers(meta, e.and),
      or: resolveMembers(meta, e.or),
    };
  });

export default {
  components: {
    QueryRenderer,
  },
  props: {
    query: {
      type: Object,
      default: () => ({}),
    },
    cubejsApi: {
      type: Object,
      required: true,
    },
    initialChartType: {
      type: String,
      default: () => 'line',
    },
    disableHeuristics: {
      type: Boolean,
    },
    stateChangeHeuristics: {
      type: Function,
    },
    initialVizState: {
      type: Object,
      default: () => ({}),
    },
    wrapWithQueryRenderer: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    const {
      query = this.query,
      chartType = this.initialChartType,
      pivotConfig,
    } = this.initialVizState;

    return {
      initialQuery: query,
      skipHeuristics: true,
      meta: undefined,
      mutex: {},
      chartType,
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
      prevValidatedQuery: null,
      granularities: GRANULARITIES,
      pivotConfig: ResultSet.getNormalizedPivotConfig(query || {}, pivotConfig),
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
          set: (memberId, newOrder) => {
            this.order = reduceOrderMembers(
              orderMembers.map((orderMember) => ({
                ...orderMember,
                order: orderMember.id === memberId ? newOrder : orderMember.order,
              }))
            );
          },
          update: (newOrder) => {
            this.order = newOrder;
          },
          reorder: (sourceIndex, destinationIndex) => {
            this.order = reduceOrderMembers(
              moveItemInArray(orderMembers, sourceIndex, destinationIndex)
            );
          },
        },
        updatePivotConfig: {
          moveItem: ({ sourceIndex, destinationIndex, sourceAxis, destinationAxis }) => {
            this.pivotConfig = movePivotItem(
              this.pivotConfig,
              sourceIndex,
              destinationIndex,
              sourceAxis,
              destinationAxis
            );
          },
          update: (pivotConfig) => {
            this.pivotConfig = {
              ...this.pivotConfig,
              ...pivotConfig,
            };
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

    if (!this.wrapWithQueryRenderer && this.$scopedSlots.builder) {
      return createElement('div', {}, [this.$scopedSlots.builder(builderProps)]);
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
        on: {
          queryStatus: (event) => {
            this.$emit('queryStatus', event);
          },
        },
      },
      children
    );
  },
  computed: {
    isQueryPresent() {
      const { validatedQuery } = this;

      return isQueryPresent(validatedQuery);
    },
    orderMembers() {
      return getOrderMembersFromOrder(
        [
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
            };
          })
          .filter(Boolean),
        this.order
      );
    },
    vizState() {
      return {
        query: this.validatedQuery,
        chartType: this.chartType,
        pivotConfig: this.pivotConfig,
      };
    },
    validatedQuery() {
      let validatedQuery = {};
      let toQuery = (member) => member.name;
      // TODO: implement timezone

      let hasElements = false;
      QUERY_ELEMENTS.forEach((element) => {
        if (element === 'timeDimensions') {
          toQuery = (member) => ({
            dimension: member.dimension.name,
            granularity: member.granularity,
            dateRange: member.dateRange,
          });
        } else if (element === 'filters') {
          toQuery = (member) => ({
            member: member.member && member.member.name,
            operator: member.operator,
            values: member.values,
            and: member.and && member.and.map(toQuery),
            or: member.or && member.or.map(toQuery),
          });
        }

        if (this[element].length > 0) {
          validatedQuery[element] = this[element].map((x) => toQuery(x));

          hasElements = true;
        }
      });

      if (validatedQuery.filters) {
        validatedQuery.filters = validateFilters(validatedQuery.filters);
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
          {
            query: validatedQuery,
            chartType: this.chartType,
          },
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

      // query heuristics should only apply on query change (not applied to the initial query)
      if (this.prevValidatedQuery !== null) {
        this.skipHeuristics = false;
      }

      this.prevValidatedQuery = validatedQuery;
      return validatedQuery;
    },
  },

  async mounted() {
    this.meta = await this.cubejsApi.meta();

    this.copyQueryFromProps();

    if (isQueryPresent(this.initialQuery)) {
      try {
        const dryRunResponse = await this.cubejsApi.dryRun(this.initialQuery);

        this.pivotConfig = ResultSet.getNormalizedPivotConfig(
          dryRunResponse?.pivotQuery || {},
          this.pivotConfig
        );
      } catch (error) {
        console.error(error);
      }
    }
  },

  methods: {
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
      } = query || this.initialQuery;

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
      const memberTypes = ['dimensions', 'measures'];
      this.filters = filters.map((m, index) => {
        const memberName = m.member || m.dimension;
        return {
          ...m,
          member: memberName && this.meta.resolveMember(memberName, memberTypes),
          operators: memberName && this.meta.filterOperatorsForMember(memberName, memberTypes),
          and: resolveMembers(this.meta, m.and),
          or: resolveMembers(this.meta, m.or),
          index,
        };
      });

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
        mem = {
          ...member,
          and: resolveMembers(this.meta, member.and),
          or: resolveMembers(this.meta, member.or),
          member: getDimensionOrMeasure(this.meta, member),
        };
      } else {
        mem = this[`available${name}`].find((m) => m.name === member);
      }

      if (mem) {
        this[element].push(mem);
      }
    },
    removeMember(element, member) {
      let index;
      if (element === 'timeDimensions') {
        index = this[element].findIndex((x) => x.name === member);
      } else if (element === 'filters') {
        index = this[element].findIndex((x) => x.member.name === member);
      } else {
        index = this[element].findIndex((x) => x.name === member);
      }
      this[element].splice(index, 1);
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
        mem = {
          ...member,
          and: resolveMembers(this.meta, member.and),
          or: resolveMembers(this.meta, member.or),
          member: getDimensionOrMeasure(this.meta, member),
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
          mem = {
            ...m,
            and: resolveMembers(this.meta, m.and),
            or: resolveMembers(this.meta, m.or),
            member: getDimensionOrMeasure(this.meta, m),
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
    updateChart(chartType) {
      this.chartType = chartType;
    },
    setOrder(order = {}) {
      this.order = order;
    },
    emitVizStateChange(partialVizState) {
      this.$emit(
        'vizStateChange',
        clone({
          ...this.vizState,
          ...partialVizState,
        })
      );
    },
  },

  watch: {
    validatedQuery: {
      deep: true,
      handler(query, prevQuery) {
        const hasQueryChanged = !areQueriesEqual(query, prevQuery);

        if (hasQueryChanged) {
          this.emitVizStateChange({
            query,
          });
        }

        if (isQueryPresent(query) && hasQueryChanged) {
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
      handler(query) {
        if (!this.meta) {
          // this is ok as if meta has not been loaded by the time query prop has changed,
          // then the promise for loading meta (found in mounted()) will call
          // copyQueryFromProps and will therefore update anyway.
          return;
        }
        this.copyQueryFromProps(query);
      },
    },
    pivotConfig: {
      deep: true,
      handler(pivotConfig, prevPivotConfig) {
        if (!equals(pivotConfig, prevPivotConfig)) {
          this.emitVizStateChange({
            pivotConfig,
          });
        }
      },
    },
    chartType(value) {
      this.emitVizStateChange({
        chartType: value,
      });
    },
  },
};
