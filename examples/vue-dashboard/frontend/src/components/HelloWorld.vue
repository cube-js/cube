<template>
  <div class="hello">
    <query-builder :cubejs-api="cubejsApi" :query="query">
      <template v-slot="{ measures, resultSet, loading }">
        <chart-renderer v-if="!loading" :result-set="resultSet" />
      </template>
    </query-builder>
  </div>
</template>

<script>
import cubejs from '@cubejs-client/core';
import { QueryBuilder } from '@cubejs-client/vue';
import ChartRenderer from './ChartRenderer.vue';

export default {
  name: 'HelloWorld',
  components: {
    QueryBuilder,
    ChartRenderer,
  },
  props: {
    msg: String,
  },
  data() {
    const cubejsApi = cubejs(
      'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.K9PiJkjegbhnw4Ca5pPlkTmZihoOm42w8bja9Qs2qJg',
      { apiUrl: 'https://react-query-builder.herokuapp.com/cubejs-api/v1' },
    );
    const query = {
      measures: ['LineItems.count', 'LineItems.quantity', 'Orders.count'],
      timeDimensions: [
        {
          dimension: 'LineItems.createdAt',
          granularity: 'month',
        },
      ],
    };

    return {
      cubejsApi,
      query,
    };
  },
};
</script>

<style scoped lang="scss">
h3 {
  margin: 40px 0 0;
}
ul {
  list-style-type: none;
  padding: 0;
}
li {
  display: inline-block;
  margin: 0 10px;
}
a {
  color: #42b983;
}
</style>
