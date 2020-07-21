<template>
  <a-dropdown>
      <a class="ant-dropdown-link" @click="e => e.preventDefault()" v-if="foundChartType">
        <a-icon :type="foundChartType.icon" />
        {{foundChartType.title}}
      </a>
      <a v-else>
        select
      </a>
    <a-menu slot="overlay">
      <a-menu-item v-for="item in ChartTypes" :key="item.title">
        <AIcon :type="item.icon"></AIcon>
        {{item.title}}
      </a-menu-item>
    </a-menu>
  </a-dropdown>
</template>

<script>
  export default {
    name: "SelectChartType",
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
      chartType: {
        type: String,
        require: false,
        default: () =>""
      },
      updateMethods: {
        type: Object,
        require: false,
        default: () => {
        }
      },
    },
    data() {
      return {
        title: "demo",
        item:"",
        ChartTypes: [
          {
            name: "line",
            title: "Line",
            icon: "line-chart"
          },
          {
            name: "area",
            title: "Area",
            icon: "area-chart"
          },
          {
            name: "bar",
            title: "Bar",
            icon: "bar-chart"
          },
          {
            name: "pie",
            title: "Pie",
            icon: "pie-chart"
          },
          {
            name: "table",
            title: "Table",
            icon: "table"
          },
          {
            name: "number",
            title: "Number",
            icon: "info-circle"
          }
        ],
      }
    },
    computed: {
      foundChartType(){
        const item =  this.ChartTypes.find(t => t.name === this.chartType);
        if(item){
          this.updateMethods['updateChartType'](item.name)
        }
        return item;
      },
      currentIndex() {
        return ''
      }
    },

    watch: {
      "title"() {},
      "itemProp":{
        immediate:true,
        handler(val){
          if(val){
            this.item = this.$common.deepCopy(val)
          }
        }
      },
    },

    async mounted() {
      setTimeout(()=>{
        this.updateMethods['updateChartType']("line")
      },200)
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
