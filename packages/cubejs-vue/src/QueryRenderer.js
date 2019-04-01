import Vue from 'vue';
import { toPairs, fromPairs } from 'ramda';

export default Vue.component('QueryRenderer', {
  props: {
    query: {
      type: Object,
      default: () => ({}),
    },
    queries: {
      type: Object,
    },
    cubejsApi: {
      type: Object,
      required: true,
    },
  },
  data() {
    return {
      mutexObj: {},
      error: undefined,
      resultSet: undefined,
      loadingState: false,
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
  // TODO: handle update
  render(createElement) {
    const { resultSet, error, loading, sqlQuery } = this;

    if (this.$slots.default) {
      return createElement(
        'div',
        this.$scopedSlots.default({
          resultSet: this.queries ? (resultSet || {}) : resultSet,
          error,
          loadingState: { isLoading: loading, },
          sqlQuery,
        }),
      );
    } else {
      return null;
    }
  },
  methods: {
    async load(query) {
      try {
        this.loading = true;

        if (query && Object.keys(query).length) {
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
});
