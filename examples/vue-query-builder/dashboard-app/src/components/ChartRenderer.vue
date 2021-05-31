<template>
  <div class="chart-renderer" v-if="resultSet">
    <component :is="componentType" :data="data" height="400px"></component>

    <Table v-if="chartType === 'table'" :result-set="resultSet"></Table>

    <div v-if="chartType === 'number'" class="number-container">
      <div v-for="item in resultSet.series()" :key="item.key">
        {{ item.series[0].value }}
      </div>
    </div>
  </div>
</template>

<script>
import { ResultSet } from '@cubejs-client/core';

import Table from './Table';

export default {
  name: 'ChartRenderer',
  props: {
    chartType: {
      type: String,
      required: true,
      default: () => 'line',
    },
    resultSet: {
      type: ResultSet,
      required: true,
    },
  },

  components: {
    Table,
  },

  computed: {
    componentType() {
      return `${this.chartType === 'bar' ? 'column' : this.chartType}-chart`;
    },

    data() {
      if (['line', 'area'].includes(this.chartType)) {
        return this.series(this.resultSet);
      }

      if (this.chartType === 'pie') {
        return this.pairs(this.resultSet);
      }

      if (this.chartType === 'bar') {
        return this.seriesPairs(this.resultSet);
      }
    },
  },

  methods: {
    series(resultSet) {
      const seriesNames = resultSet.seriesNames();
      const pivot = resultSet.chartPivot();
      const series = [];

      seriesNames.forEach((e) => {
        const data = pivot.map((p) => [p.x, p[e.key]]);
        series.push({ name: e.key, data });
      });
      return series;
    },
    pairs(resultSet) {
      return resultSet.series()[0].series.map((item) => [item.x, item.value]);
    },
    seriesPairs(resultSet) {
      return resultSet.series().map((seriesItem) => ({
        name: seriesItem.title,
        data: seriesItem.series.map((item) => [item.x, item.value]),
      }));
    },
  },
};
</script>

<style>
.chart-renderer {
  width: 100%;
  height: 400px;
}

.number-container {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
}
</style>
