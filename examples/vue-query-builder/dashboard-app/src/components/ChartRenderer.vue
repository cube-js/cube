<template>
  <div class="chart-renderer" v-if="resultSet">
    <line-chart legend="bottom" v-if="chartType === 'line'" :data="series(resultSet)"></line-chart>

    <area-chart legend="bottom" v-if="chartType === 'area'" :data="series(resultSet)"></area-chart>

    <pie-chart v-if="chartType === 'pie'" :data="pairs(resultSet)"></pie-chart>

    <column-chart v-if="chartType === 'bar'" :data="seriesPairs(resultSet)"></column-chart>

    <Table v-if="chartType === 'table'" :data="resultSet"></Table>

    <div v-if="chartType === 'number'">
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
    Table
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

<style scoped>
.chart-renderer {
  width: 100%;
}
</style>
