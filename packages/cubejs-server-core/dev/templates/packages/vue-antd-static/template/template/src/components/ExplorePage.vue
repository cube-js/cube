<template>
    <div>
        <ExploreQueryBuilder
                :query-prop="query"
        />
    </div>
</template>
<script>
    import ExploreQueryBuilder from "~/components/cube/ExploreQueryBuilder";

    export default {
        name: "ExplorePage",
        layout: 'empty',
        auth: 'false',
        components: {ExploreQueryBuilder},
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
                query: {
                    measures: []
                },
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
