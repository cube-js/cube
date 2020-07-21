<template>
  <div class="hello">
    <v-chart :options="options" ></v-chart>
  </div>
</template>

<script>

  import 'echarts/lib/chart/line'

  export default {
    name: "ChartItem",
    layout: 'empty',
    auth: 'false',
    components: {},
    props: {
      itemProp: {
        type: Object,
        require: false,
        default: () => {
        }
      },
      list: {
        type: Array,
        require: false,
        default: () => []
      },
      cubejsApi: {
        type: Object,
        require: false,
        default: () => {
        }
      },
      resultSet: {
        type: Object,
        require: false,
        default: () => []
      },
      chartType: {
        type: String,
        require: false,
        default: () => ""
      },
    },
    data() {

      return {
        title: "demo",
        item: "",
      }
    },
    computed: {
      options() {
        const seriesNames = this.resultSet.seriesNames();
        const pivot = this.resultSet.chartPivot();

        const series = [];
        seriesNames.forEach((e) => {
          const data = pivot.map(p => {
            const o = {value:p[e.key],
            name: p.x};
            return o;
          });
          series.push({ name: e.key, data,  type: 'line' , showSymbol: false});
        });
          let options = {}
        options =  {
          xAxis: {
            data: pivot.map(i => i.x)
          },
          yAxis: {},
          series:series
        };
        return options;
      },
    },

    watch: {
      "title"() {
      },
      "itemProp": {
        immediate: true,
        handler(val) {
          if (val) {
            this.item = this.$common.deepCopy(val)

          }
        }
      },
    },

    async mounted() {

    },
    methods: {
      async asyncMounted() {


      },
      async methodDemo() {

      },
      async dispatch() {
        this.$store.dispatch('modules/common/setCurrentIndex', 1)
      },
    }
  }
</script>

<style lang='scss'>

</style>
