<template>
    <a-button-group style="margin-right: 8px">
      <a-button>
        <slot></slot>
      </a-button>

      <a-button type="danger" ghost @click="onRemoveClick" >
        <a-icon type="close" />
      </a-button>
    </a-button-group>
</template>

<script>

  export default {
    name: "RemoveButtonGroup",
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
    },
    data() {

      return {
        title: "demo",
        item:"",
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
    },

    async mounted() {

    },
    methods: {
      onRemoveClick(){
        this.$emit("onRemoveClick")
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
