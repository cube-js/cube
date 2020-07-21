<template>
  <AModal
    key="modal"
    title="Save Chart"
    :visible=titleModalVisible
    @ok=onOk
    @cancel=cancel
  >
    <AInput
      v-model="finalTitle"
      placeholder="Dashboard Item Name"
      @onChange=onChange
    />
  </AModal>
</template>

<script>




  export default {
    name: "TitleModal",
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
        item: "",
        finalTitle: "",
        titleModalVisible:true,
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
      titleModalVisible(val) {
        this.$emit("input",val)
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
      onChange(){
      },
      async onOk() {
        this.titleModalVisible = false
        const obj = {}
        obj.title = this.finalTitle
        this.$emit("submit",obj)
      },
      async cancel() {
        this.titleModalVisible = false
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
