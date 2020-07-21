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
            {{m.dimension.title}}
    </MemberDropdown>
    </RemoveButtonGroup>

      <!--时间-->
       <b :key="`${m.dimension.name}-for`">FOR</b>
       <a-button>
          <ButtonDropdown
            :availableMembersProp=DateRanges
            style="margin: 0px 8px"
            :key="`${m.dimension.name}-date-range`"
            @updateMethodsAdd="(dateRange)=>updateDateRange(m,{...m,dateRange: dateRange.value})"
          >
              {{m.dateRange || "All time"}}
           </ButtonDropdown>
      </a-button>

       <b :key="`${m.dimension.name}-by`">BY</b>
          <a-button>
         <ButtonDropdown
           :availableMembersProp=m.dimension.granularities
           style="margin-left: 8px"
           :key="`${m.dimension.title}-granularity`"
           @updateMethodsAdd="(granularity)=>updateDateRange(m, { ...m, granularity: granularity.name })"

         >
          {{m.dimension.granularities.find(g => g.name === m.granularity) &&
            m.dimension.granularities.find(g => g.name === m.granularity).title}}
         </ButtonDropdown>
          </a-button>
  </span>

    <span v-show="membersProp.length==0">
      <a-button>
          <MemberDropdown
          :available-members-prop="availableMembersProp"
          @updateMethodsAdd="add"
          >
          {{addMemberNameProp}}
      </MemberDropdown>
      </a-button>
    </span>
  </span>
</template>

<script>
  import RemoveButtonGroup from "~/components/cube/QueryBuilder/RemoveButtonGroup";
  import MemberDropdown from "~/components/cube/QueryBuilder/MemberDropdown";
  import ButtonDropdown from "~/components/cube/QueryBuilder/ButtonDropdown";
  export default {
    name: "TimeGroup",
    layout: 'empty',
    auth: 'false',
    components: {ButtonDropdown, MemberDropdown, RemoveButtonGroup},
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
        DateRanges: [
          {
            title: "All time",
            value: undefined
          },
          {
            value: "Today"
          },
          {
            value: "Yesterday"
          },
          {
            value: "This week"
          },
          {
            value: "This month"
          },
          {
            value: "This quarter"
          },
          {
            value: "This year"
          },
          {
            value: "Last 7 days"
          },
          {
            value: "Last 30 days"
          },
          {
            value: "Last week"
          },
          {
            value: "Last month"
          },
          {
            value: "Last quarter"
          },
          {
            value: "Last year"
          }
        ],
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
      remove(m) {
        this.updateMethods["remove" + this.addMemberNameProp + "s"](m.name)
      },
      updateDateRange(oldM, newM) {
        const data = {
          dimension: newM.dimension.name,
          granularity: newM.granularity,
          dateRange: newM.dateRange
        }
        this.updateMethods["update" + this.addMemberNameProp + "s"](oldM, data)
      },

      add(m) {
        const data ={
          dimension: m.name,
          granularity: "day"
        };
        this.updateMethods["add" + this.addMemberNameProp + "s"](data)
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
