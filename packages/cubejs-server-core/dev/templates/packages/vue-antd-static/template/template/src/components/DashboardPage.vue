<template>
    <div style="width: 100%">
        <Dashboard v-show="list.length>0" superRef="Dashboard" @setList="(list)=>{this.list = list}"></Dashboard>

        <div  v-show="list.length==0"
                style="text-align: center;padding: 12px"
        >
            <h2>There are no charts on this dashboard</h2>
            <a-button to="/explore">
                <AButton type="primary" size="large">
                    <AIcon type="plus"></AIcon>
                    Add chart
                </AButton>
            </a-button>
        </div>
    </div>
</template>

<script>

    const Dashboard = function () {
        try {
            console.log(Element)
        } catch (e) {
            return "";
        }
        return import('~/components/cube/Dashboard');
    }


    export default {
        name: "DashboardPage",
        layout: 'empty',
        auth: 'false',
        components: {Dashboard},
        props: {
            itemProp: {
                type: Object,
                require: false,
                default: () => {
                }
            }
        },
        data() {

            return {
                title: "demo",
                item: "",
                list:[],
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
