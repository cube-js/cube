<template>
    <div>
        <grid-layout
                :layout.sync="layout"
                :col-num="12"
                :row-height="50"
                :is-draggable="true"
                :is-resizable="true"
                :is-mirrored="false"
                :vertical-compact="true"
                :margin="[10, 10]"
                :use-css-transforms="true"
                @layout-updated="layoutUpdatedEvent"
                @layout-ready="layoutReadyEvent">
            <grid-item v-for="item in list"
                       style="background: antiquewhite;color:green"
                       :x="item.layout.x"
                       :y="item.layout.y"
                       :w="item.layout.w"
                       :h="item.layout.h"
                       :i="item.layout.i"
                       :key="item.layout.i">
                <div>
                    <DashboardItem :item-prop="item"></DashboardItem>
                </div>
            </grid-item>
        </grid-layout>
    </div>
</template>

<script>
    import VueGridLayout from 'vue-grid-layout';
    import Action from '~/plugins/localStorageMutation';
    import DashboardItem from "~/components/cube/DashboardItem";

    export default {
        name: "Dashboard",
        layout: 'empty',
        auth: 'false',
        components: {
            DashboardItem,
            GridLayout: VueGridLayout.GridLayout,
            GridItem: VueGridLayout.GridItem
        },
        props: {
            itemProp: {
                type: Object,
                require: false,
                default: () => {
                }
            },
            listProp: {
                type: Array,
                require: false,
                default: () => []
            },
        },
        data() {

            return {
                title: "demo",
                list: [],
                item: "",
                layout: [],
                layoutMap: [],
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
            this.getList();
        },
        methods: {
            getList() {
                const list = Action.Query.dashboardItems();
                const layoutItems = []

                for (let i = 0; i < list.length; i++) {
                    const item = list[i]
                    if (item.layout == null || item.layout === {} || item.layout === '{}') {
                        item.layout = {}
                        item.layout.x = this.addItem().x;
                        item.layout.y = this.addItem().y;
                        item.layout.h = this.addItem().h;
                        item.layout.w = this.addItem().w;
                        item.layout.i = item.id
                    }
                    layoutItems.push(item.layout)
                }

                this.layout = layoutItems;
                Action.Mutation.setDashboardItems(list)

                console.info("this.layout", this.layout)

                this.list = list;
                this.$emit("setList", list)
            },

            addItem: function () {
                // Generate random width and height
                var itemW = this.rnd(5, 10);
                var itemH = this.rnd(5, 10);
                var addItem = {
                    "x": 0,
                    "y": this.layoutMap.length,
                    "w": itemW,
                    "h": itemH,
                    "i": this.layout[this.layout.length - 1] ? parseInt(this.layout[this.layout.length - 1].i) + 1 : 0
                };
                if (this.layoutMap.length) {
                    // console.log(this.layoutMap.length);
                    for (let r = 0, rLen = this.layoutMap.length; r < rLen; r++) {
                        for (let c = 0; c <= (this.layoutColNum - itemW); c++) {
                            let res = this.regionalTest(
                                c,
                                r,
                                itemW,
                                rLen > (r + itemH) ? itemH : rLen - r
                            );

                            if (res.result) {
                                // Update add data content
                                addItem = {
                                    "x": res.x,
                                    "y": res.y,
                                    "w": itemW,
                                    "h": itemH,
                                    "i": parseInt(this.layout[this.layout.length - 1].i) + 1
                                };
                                c = this.layoutColNum + 1;
                                r = rLen + 1;
                            } else {
                                c = res.offsetX;
                            }
                        }
                    }
                }
                // Update 2D array map
                for (let itemR = 0; itemR < itemH; itemR++) {
                    for (let itemC = 0; itemC < itemW; itemC++) {
                        // If there is no row, initialize
                        if (!this.layoutMap[addItem.y + itemR]) {
                            this.layoutMap[addItem.y + itemR] = new Array(this.layoutColNum);
                            for (let i = 0; i < this.layoutColNum; i++) {
                                this.layoutMap[addItem.y + itemR][i] = 0;
                            }
                        }
                        // Marking point
                        this.layoutMap[addItem.y + itemR][addItem.x + itemC] = 1;
                    }
                }
                // Add data
                return addItem;

            },
            rnd: function (m, n) {
                return (Math.random() * (m - n + 1) + n) | 0;
            },
            layoutReadyEvent: function () {
                this.layoutMap = this.genereatePlaneArr(this.layout);
            },
            layoutUpdatedEvent: function () {
                this.layoutMap = this.genereatePlaneArr(this.layout);
                this.list = this.layout.map((item) => {
                    for (let i = 0; i < this.list.length; i++) {
                        if (item.i === this.list[i].id) {
                            this.list[i].layout = item;
                            return this.list[i];
                        }
                    }
                })
                Action.Mutation.setDashboardItems(this.list)
            },

            genereatePlaneArr: function (data) {
                var map = [];
                if (Array.isArray(data)) {
                    for (var i = 0; i < data.length; i++) {
                        var one = data[i];
                        for (var r = one.y; r < (one.y + one.h); r++) {
                            for (var c = one.x; c < (one.x + one.w); c++) {
                                if (!map[r]) {
                                    map[r] = new Array(this.layoutColNum);

                                    for (let i = 0; i < this.layoutColNum; i++) {
                                        map[r][i] = 0;
                                    }
                                }
                                map[r][c] = 1;
                            }
                        }
                    }
                }
                return map;
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
