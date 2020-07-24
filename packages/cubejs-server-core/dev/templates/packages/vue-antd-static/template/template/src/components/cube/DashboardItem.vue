<template>
    <ACard
            style="width: 100%;height: 100%"
            :title="item.name">
        <a-dropdown slot="extra">
            <a-button shape="circle" icon="menu" type="menu"/>
            <AMenu slot="overlay">
                <!--          <a-menu-item>
                              <a-button>Edit</a-button>
                          </a-menu-item>-->
                <AMenuItem
                        @click="del(item)">
                    Delete
                </AMenuItem>
            </AMenu>
        </a-dropdown>

        <div style="overflow: hidden">
            <ChartRenderer
                    :query="item.vizState"
                    :chartType="item.chartType"
                    :cubejsApi=cubejsApi
            />
        </div>
    </ACard>
</template>

<script>

    import Action from "~/plugins/localStorageMutation";
    import ChartRenderer from "~/components/cube/ChartRenderer";
    import cubejs from "@cubejs-client/core";

    const cubejsApi = cubejs(
        '1c1ebc97f25e09b76fab81155adaddca99ad483e64b2fc0e0eb2a1574d988a5b81a5b37f20386b1ca8cf2e78d22b5c33757150adb99d5ea7b809368a721a194a',
        {apiUrl: 'http://localhost:4000/cubejs-api/v1'},
    );

    export default {
        name: "DashboardItem",
        layout: 'empty',
        auth: 'false',
        components: {ChartRenderer},
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
                cubejsApi,
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
                        this.item = JSON.parse(JSON.stringify(val))
                    }
                }
            },
        },

        async mounted() {

        },
        methods: {
            del(item) {
                const self = this;

                console.info(this)
                this.$confirm({
                    title: "Are you sure you want to delete this item?",
                    okText: "Yes",
                    okType: "danger",
                    cancelText: "No",

                    onOk() {
                        Action.Mutation.deleteDashboardItem(item)
                        setTimeout(() => {
                            self.$superVue.callMethod("Dashboard", "getList", null)
                        }, 200)
                    }
                })
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
