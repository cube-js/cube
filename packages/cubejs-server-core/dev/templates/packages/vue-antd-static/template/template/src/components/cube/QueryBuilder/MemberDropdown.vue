<template>

  <div>
    <a-dropdown>
      <a class="ant-dropdown-link" @click="e => e.preventDefault()">

        <slot></slot>
         <a-icon type="down" />
      </a>
      <a-menu slot="overlay">
        <a-menu-item v-for="m in availableMembersProp" :key="m.name"
                     @click="updateMethodsAdd(m)"
        >
          <a href="javascript:;">{{m.title}}</a>
        </a-menu-item>
      </a-menu>
    </a-dropdown>
  </div>
</template>

<script>



  export default {
    name: "MemberDropdown",
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
      availableMembersProp: {
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
      updateMethodsAdd(m){
        this.$emit("updateMethodsAdd",m)
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
