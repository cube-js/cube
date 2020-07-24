<template>
  <div class="hello">
    <query-renderer :cubejs-api="cubejsApi" :query="query" v-if="cubejsApi">
      <template v-slot="{ resultSet }">
        <ChartItem :resultSet="resultSet"></ChartItem>
      </template>
      <template v-slot:empty>
        <div class="loading-container">
        </div>
      </template>
    </query-renderer>
  </div>
</template>

<script>
  import {QueryRenderer} from '@cubejs-client/vue';
  import ChartItem from "~/components/cube/ChartItem";
  export default {
    name: "ChartRenderer",
    layout: 'empty',
    auth: 'false',
    components: {ChartItem, QueryRenderer},
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
      query: {
        type: Object,
        require: false,
        default: () => []
      },
      chartType: {
        type: String,
        require: false,
        default: () => "line"
      },
    },
    data() {

      return {
        title: "demo",
        item: "",
      }
    },
    computed: {
      currentIndex() {
        return ''

      }
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
