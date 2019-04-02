<template>
  <div class="chart-renderer">
    <line-chart :data="series"></line-chart>
  </div>
</template>

<script>
export default {
  name: 'ChartRenderer',
  props: {
    resultSet: {
      type: Object,
      required: true,
    },
  },
  computed: {
    series() {
      const seriesNames = this.resultSet.seriesNames();
      const pivot = this.resultSet.chartPivot();
      const series = [];
      seriesNames.forEach((e) => {
        const data = pivot.map(p => [p.x, p[e.key]]);
        series.push({ name: e.key, data });
      });
      return series;
    },
  },
};
</script>

<style>

</style>
