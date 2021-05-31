import { toPairs, fromPairs } from 'ramda';
import { isQueryPresent, areQueriesEqual } from '@cubejs-client/core';

export default {
  props: {
    query: {
      type: [Object, Array],
      default: () => ({}),
    },
    // TODO: validate with current react implementation
    queries: {
      type: Object,
    },
    loadSql: {
      required: false,
    },
    cubejsApi: {
      type: Object,
      required: true,
    },
    builderProps: {
      type: Object,
      required: false,
      default: () => ({}),
    },
    chartType: {
      type: String,
      required: false,
    },
  },
  data() {
    return {
      mutexObj: {},
      error: undefined,
      resultSet: undefined,
      loading: false,
      sqlQuery: undefined,
    };
  },
  async mounted() {
    const { query, queries } = this;

    if (isQueryPresent(query)) {
      await this.load();
    } else if (isQueryPresent(queries)) {
      await this.loadQueries(queries);
    }
  },
  render(createElement) {
    const { $scopedSlots, resultSet, error, loading, sqlQuery } = this;
    const empty = createElement('div', {});
    let slot = this.$slots.empty ? this.$slots.empty : empty;
    let controls = createElement('div', {});
    const onlyDefault = !('empty' in this.$slots) && !('error' in this.$scopedSlots);

    if ($scopedSlots.builder && this.builderProps.measures) {
      controls = $scopedSlots.builder({ ...this.builderProps });
    }

    if ((!loading && resultSet && !error) || onlyDefault) {
      let slotProps = {
        resultSet,
        sqlQuery,
        query: this.builderProps.query || this.query,
      };

      if (onlyDefault) {
        slotProps = {
          loading,
          error,
          refetch: this.load,
          ...this.builderProps,
          ...slotProps,
        };
      }

      slot = $scopedSlots.default ? $scopedSlots.default(slotProps) : slot;
    } else if (error) {
      slot = $scopedSlots.error ? $scopedSlots.error({ error, sqlQuery }) : slot;
    }

    return createElement('div', {}, [controls, slot]);
  },
  methods: {
    async load() {
      const { query } = this;

      if (!isQueryPresent(query)) {
        return;
      }

      try {
        this.loading = true;
        this.error = null;
        this.resultSet = null;

        if (this.loadSql === 'only') {
          this.sqlQuery = await this.cubejsApi.sql(query, {
            mutexObj: this.mutexObj,
            mutexKey: 'sql',
          });
        } else if (this.loadSql) {
          this.sqlQuery = await this.cubejsApi.sql(query, {
            mutexObj: this.mutexObj,
            mutexKey: 'sql',
          });
          this.resultSet = await this.cubejsApi.load(query, {
            mutexObj: this.mutexObj,
            mutexKey: 'query',
          });
        } else {
          this.resultSet = await this.cubejsApi.load(query, {
            mutexObj: this.mutexObj,
            mutexKey: 'query',
          });
        }

        this.loading = false;
      } catch (error) {
        this.error = error;
        this.resultSet = undefined;
        this.loading = false;
      }
    },
    async loadQueries() {
      const { queries } = this;
      try {
        this.error = undefined;
        this.loading = true;

        const resultPromises = Promise.all(
          toPairs(queries).map(([name, query]) =>
            this.cubejsApi
              .load(query, {
                mutexObj: this.mutexObj,
                mutexKey: name,
              })
              .then((r) => [name, r])
          )
        );

        this.resultSet = fromPairs(await resultPromises);
        this.loading = false;
      } catch (error) {
        this.error = error;
        this.loading = false;
      }
    },
  },
  watch: {
    loading(loading) {
      if (loading === false) {
        this.$emit('queryStatus', {
          isLoading: false,
          error: this.error,
          resultSet: this.resultSet,
        });
      } else {
        this.$emit('queryStatus', { isLoading: true });
      }
    },
    cubejsApi() {
      this.load();
    },
    chartType() {
      this.load();
    },
    query: {
      deep: true,
      handler(query, prevQuery) {
        if (!areQueriesEqual(query, prevQuery)) {
          this.load();
        }
      },
    },
    queries: {
      handler(val) {
        if (val) {
          this.loadQueries();
        }
      },
      deep: true,
    },
  },
};
