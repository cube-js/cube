<template>
  <div class="container">
    <div v-if="cubejsApi && chartingLibrary">
      <query-renderer :cubejsApi="cubejsApi" :query="query" @queryLoad="handleQueryLoad">
        <template #default="{ resultSet }">
          <chart-renderer
            v-if="resultSet"
            :component="chartingLibrary"
            :chart-type="chartType"
            :result-set="resultSet"
          ></chart-renderer>
        </template>
      </query-renderer>
    </div>

    <div>
      <v-btn @click="changeQuery()">change query</v-btn>
      <v-btn @click="changeChartType()">change chart type</v-btn>
      <v-btn @click="changeToken()">change token</v-btn>
      <v-btn @click="changeLibrary()">change library</v-btn>
    </div>
  </div>
</template>

<script>
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/vue';

import ChartRenderer from './ChartRenderer';

// const API_URL = 'https://ecom.cubecloudapp.dev/cubejs-api/v1';
const API_URL = 'http://localhost:4000/cubejs-api/v1';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTQ2NjY4OTR9.0fdi5cuDZ2t3OSrPOMoc3B1_pwhnWj4ZmM3FHEX7Aus';

export default {
  name: 'ChartContainer',

  data() {
    return {
      chartingLibrary: null,
      query: {},
      pivotConfig: null,
      chartType: 'line',
      apiUrl: API_URL,
      token: CUBEJS_TOKEN,
      cubejsApi: null,
    };
  },

  mounted() {
    this.token = 'test.token';
    this.query = {
      measures: ['Sales.count'],
    };
    window.addEventListener('__cubejsPlaygroundEvent', (event) => {
      const { query, chartingLibrary, chartType, pivotConfig, eventType } = event.detail;

      if (eventType === 'chart') {
        if (query) {
          this.query = query;
        }
        if (pivotConfig) {
          this.pivotConfig = pivotConfig;
        }
        if (chartingLibrary) {
          this.chartingLibrary = chartingLibrary;
        }
        if (chartType) {
          this.chartType = chartType;
        }
      } else if (eventType === 'credentials') {
        // updateVersion((prev) => prev + 1);
      }
    });

    const { onChartRendererReady } = window.parent.window['__cubejsPlayground'] || {};
    if (typeof onChartRendererReady === 'function') {
      onChartRendererReady();
    }
  },

  methods: {
    handleQueryLoad({ resultSet, error }) {
      const { onQueryLoad } = window.parent.window['__cubejsPlayground'] || {};
      if (typeof onQueryLoad === 'function') {
        onQueryLoad({ resultSet, error })
      }
    },

    changeQuery() {
      this.query = {
        measures: ['Sales.count'],
        timeDimensions: [
          {
            dimension: 'Sales.ts',
            granularity: 'month',
            // dateRange: 'This quarter',
          },
        ],
      };
    },

    changeToken() {
      this.token = Math.random().toString();
    },

    changeChartType() {
      if (this.chartType === 'line') {
        this.chartType = 'area';
      } else {
        this.chartType = 'line';
      }
    },

    changeLibrary() {
      if (this.chartingLibrary === 'chartkick') {
        this.chartingLibrary = 'chartjs';
      } else {
        this.chartingLibrary = 'chartkick';
      }
    },
  },

  computed: {
    credentials() {
      return [this.apiUrl, this.token].join();
    },
  },

  watch: {
    credentials(value) {
      this.cubejsApi = cubejs(this.token, {
        apiUrl: this.apiUrl,
      });
      console.log('@change:credentials', value);
    },
  },

  components: {
    QueryRenderer,
    ChartRenderer,
  },
};
</script>

<style scoped>
.container {
  padding: 40px 80px;
}
</style>
