<template>
    <div>
    <span :key=index
          v-for="(m,index) in membersProp">

        <RemoveButtonGroup
                @onRemoveClick="()=>{remove(m)}"
        >
          <MemberDropdown
                  :availableMembersProp=availableMembersProp
                  @updateMethodsAdd="(updateWith)=>updateDateRange(m,{...m,dimension: updateWith})"
          >
            {{m.member.title}}
    </MemberDropdown>
    </RemoveButtonGroup>
      <ASelect
              @change="(operator) => updateDateRange(m, { ...m, operator })"
              style="width: 200px;margin-right: 8px"
              :value="m.operator">
        <a-select-option v-for="operator in m.operators" :key="operator.name" :value="operator.name">
            {{ operator.title }}
       </a-select-option>

      </ASelect>
      <FilterInput v-model="m.values" @change="(values)=>{
        updateDateRange(m,{...m,values: values})
      }">
      </FilterInput>
  </span>
        <span>
      <a-button>
          <MemberDropdown
                  :available-members-prop="availableMembersProp"
                  @updateMethodsAdd="add"
          >
          {{addMemberNameProp}}
      </MemberDropdown>
      </a-button>

    </span>
    </div>
</template>

<script>

    import RemoveButtonGroup from "~/components/cube/QueryBuilder/RemoveButtonGroup";
    import MemberDropdown from "~/components/cube/QueryBuilder/MemberDropdown";
    import FilterInput from "~/components/cube/QueryBuilder/FilterInput";

    export default {
        name: "FilterGroup",
        layout: 'empty',
        auth: 'false',
        components: {FilterInput, MemberDropdown, RemoveButtonGroup},
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
                this.updateMethods["remove" + this.addMemberNameProp + "s"](m.dimension)
            },
            updateDateRange(oldM, newM) {
                this.updateMethods["update" + this.addMemberNameProp + "s"](oldM, newM)

            },

            add(m) {
                const data = {
                    dimension: m.name
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
