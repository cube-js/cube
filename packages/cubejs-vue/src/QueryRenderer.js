import { toPairs, fromPairs } from 'ramda';

export default {
  props: {
    query: {
      type: Object,
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
  },
  data() {
    return {
      mutexObj: {},
      error: undefined,
      resultSet: undefined,
      loading: true,
      sqlQuery: undefined,
    };
  },
  async mounted() {
    const { query, queries } = this;

    if (query) {
      await this.load(query);
    }

    if (queries) {
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
      const slotProps = {
        resultSet,
        sqlQuery,
        query: this.builderProps.query,
      };

      if (onlyDefault) {
        Object.assign(slotProps, {
          loading,
          error,
          ...this.builderProps,
        });
      }

      slot = $scopedSlots.default ? $scopedSlots.default(slotProps) : slot;
    } else if (error) {
      slot = $scopedSlots.error ? $scopedSlots.error({ error, sqlQuery }) : slot;
    }

    return createElement(
      'div',
      {},
      [
        controls,
        slot,
      ],
    );
  },
  methods: {
    async load(query) {
      try {
        this.loading = true;
        this.error = undefined;

        if (query && Object.keys(query).length > 0) {
          if (this.loadSql === 'only') {
            this.sqlQuery = await this.cubejsApi.sql(query, { mutexObj: this.mutexObj, mutexKey: 'sql' });
          } else if (this.loadSql) {
            this.sqlQuery = await this.cubejsApi.sql(query, { mutexObj: this.mutexObj, mutexKey: 'sql' });
            this.resultSet = await this.cubejsApi.load(query, { mutexObj: this.mutexObj, mutexKey: 'query' });
          } else {
            this.resultSet = await this.cubejsApi.load(query, { mutexObj: this.mutexObj, mutexKey: 'query' });
          }
        }

        this.loading = false;
      } catch (exc) {
        this.error = exc;
        this.resultSet = undefined;
        this.loading = false;
      }
    },
    async loadQueries(queries) {
      try {
        this.error = undefined;
        this.loading = true;

        const resultPromises = Promise.all(toPairs(queries).map(
          ([name, query]) =>
          this.cubejsApi.load(query, { mutexObj: this.mutexObj, mutexKey: name }).then(r => [name, r])
        ));

        this.resultSet = fromPairs(await resultPromises);
        this.loading = false;
      } catch (exc) {
        this.error = exc;
        this.loading = false;
      }
    },
  },
  watch: {
    query(val) {
      if (val) {
        this.load(val);
      }
    },
    queries(val) {
      if (val) {
        this.loadQueries(val);
      }
    },
  },
};
