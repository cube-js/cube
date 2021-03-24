<template>
  <component v-bind:is="component" :resultSet="resultSet" :chartType="chartType" />
</template>
<script>
import { ResultSet } from '@cubejs-client/core';

import VueChartkickRenderer from '@/components/ChartRenderer';
import VueChartjsRenderer from '@/pages/test/vue-chartjs-charts/ChartRenderer';

export default {
  name: 'ChartRenderer',
  props: {
    component: {
      type: String,
      required: true,
    },
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
    chartkick: VueChartkickRenderer,
    chartjs: VueChartjsRenderer,
  },
};
</script>
