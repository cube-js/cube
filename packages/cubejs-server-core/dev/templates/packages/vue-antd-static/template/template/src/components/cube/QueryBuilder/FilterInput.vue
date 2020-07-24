<template>
  <a-select
    mode="tags"
    :size="size"
    placeholder="Please select"
    :default-value="defaultValue"
    style="width: 200px"
    @change="handleChange"
  >
  </a-select>
</template>

<script>
  export default {
    name: "FilterInput",
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
      value: {
        type: Array,
        require: false,
        default: () => []
      },
    },
    data() {
      return {
        title: "demo",
        item:"",
        size: 'default',
        defaultValue:[],
      }
    },
    computed: {
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
      "value":{
        immediate:true,
        handler(val){
          if(val){
            this.defaultValue = JSON.parse(JSON.stringify(val))
          }
        }
      },
    },

    async mounted() {


    },
    methods: {
      handleChange(value) {
        this.$emit("change",value)

      },
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
