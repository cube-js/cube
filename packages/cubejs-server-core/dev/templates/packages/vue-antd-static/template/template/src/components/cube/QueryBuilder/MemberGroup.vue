<template>
  <span>
    <span :key=index
          v-for="(m,index) in membersProp">
      <RemoveButtonGroup
        @onRemoveClick="()=>{remove(m)}"
      >
        <MemberDropdown
          :availableMembersProp=availableMembersProp
          @updateMethodsAdd="(newM)=>update(m,newM)"
        >
          {{m.title}}
    </MemberDropdown>
    </RemoveButtonGroup>
  </span>

    <a-button>
    <MemberDropdown
      :available-members-prop="availableMembersProp"
      type="dashed"
      @updateMethodsAdd="add"
    >
      {{addMemberNameProp}}
    </MemberDropdown>

    </a-button>
  </span>

</template>

<script>


  import RemoveButtonGroup from "~/components/cube/QueryBuilder/RemoveButtonGroup";
  import MemberDropdown from "~/components/cube/QueryBuilder/MemberDropdown";

  export default {
    name: "MemberGroup",
    layout: 'empty',
    auth: 'false',
    components: {MemberDropdown, RemoveButtonGroup},
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
      membersProp: {
        type: Array,
        require: false,
        default: () => []
      },
      availableMembersProp: {
        type: Array,
        require: false,
        default: () => []
      },
      addMemberNameProp: {
        type: String,
        require: false,
        default: () => ""
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
      remove(m){
        this.updateMethods["remove"+this.addMemberNameProp+"s"](m.name)
      },
      update(oldM,newM){
        this.updateMethods["update"+this.addMemberNameProp+"s"](oldM.name,newM.name)

      },
      add(m){
        console.info("mmmmmmm--->",m)
       this.updateMethods["add"+this.addMemberNameProp+"s"](m.name)

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
