<template>
  <div class="chart-renderer" v-if="resultSet">
    <line-chart legend="bottom" v-if="chartType === 'line'" :data="data(resultSet)"></line-chart>
  </div>
</template>

<script>
import { ResultSet } from '@cubejs-client/core';
import LineChart from './LineChart';

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
    LineChart,
  },
  methods: {
    data(resultSet) {
      // const seriesNames = resultSet.seriesNames();
      // const pivot = resultSet.chartPivot();
      // const series = [];
      //
      // seriesNames.forEach((e) => {
      //   const data = pivot.map((p) => [p.x, p[e.key]]);
      //   series.push({ name: e.key, label: e.key, data });
      // });
      // return series;
    },
    series(resultSet) {
      const seriesNames = resultSet.seriesNames();
      const pivot = resultSet.chartPivot();
      const series = [];

      seriesNames.forEach((e) => {
        const data = pivot.map((p) => [p.x, p[e.key]]);
        series.push({ name: e.key, label: e.key, data });
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

<style scoped>
.chart-renderer {
  width: 100%;
  height: 400px;
  max-height: 400px;
}
</style>
