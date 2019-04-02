'use strict';

Vue.component('query-builder', cubejsVue.QueryBuilder);
Vue.use(VueChartkick, { adapter: Chart });

const cubejsApi = cubejs(
'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.K9PiJkjegbhnw4Ca5pPlkTmZihoOm42w8bja9Qs2qJg',
  { apiUrl: 'https://react-query-builder.herokuapp.com/cubejs-api/v1' },
);

const example = {
  template: `
    <div class="hello">
      <query-builder :cubejs-api="cubejsApi" :query="query">
        <template v-slot="{ measures, resultSet, loading }">
          <line-chart :data="series(resultSet)"></line-chart>
        </template>
      </query-builder>
    </div>
  `,
  data() {
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
  methods: {
    series(resultSet) {
      const seriesNames = resultSet.seriesNames();
      const pivot = resultSet.chartPivot();
      const series = [];
      seriesNames.forEach((e) => {
        const data = pivot.map(p => [p.x, p[e.key]]);
        series.push({ name: e.key, data });
      });
      return series;
    },
  },
};

new Vue({
  render: h => h(example),
}).$mount('#app');
