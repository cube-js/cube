<template>
    <div>

        <TitleModal
                @submit="submit"
                v-model="titleModalVisible"
                v-if="titleModalVisible"
                history={history}
                itemId={itemId}
                setTitleModalVisible={setTitleModalVisible}
                setAddingToDashboard={setAddingToDashboard}
                finalVizState={finalVizState}
                setTitle={setTitle}
                finalTitle={finalTitle}
        />

        <query-builder ref=queryBuilder :cubejs-api="cubejsApi" :query="query">
            <template v-slot:builder="scope">
                <!--query-->
                <ARow
                        type="flex"
                        justify="space-around"
                        align="top"
                        :gutter=24
                        style="margin-bottom: 12px"
                >
                    <ACol :span=24>
                        <ACard>
                            <ARow
                                    type="flex"
                                    justify="space-around"
                                    align="top"
                                    :gutter=24
                                    style="margin-bottom: 12px"

                            >
                                <ACol :span=24>
                                    <MemberGroup
                                            :members-prop="scope.measures"
                                            :available-members-prop="scope.availableMeasures"
                                            add-member-name-prop="Measure"
                                            :update-methods="scope"
                                    />

                                    <ADivider type="vertical"/>
                                    <MemberGroup
                                            :members-prop="scope.dimensions"
                                            :available-members-prop="scope.availableDimensions"
                                            add-member-name-prop="Dimension"
                                            :update-methods="scope"
                                    />
                                    <ADivider type="vertical"/>

                                    <MemberGroup
                                            :members-prop="scope.segments"
                                            :available-members-prop="scope.availableSegments"
                                            add-member-name-prop="Segment"
                                            :update-methods="scope"
                                    />
                                    <ADivider type="vertical"/>

                                    <TimeGroup
                                            :members-prop="scope.timeDimensions"
                                            :available-members-prop="scope.availableTimeDimensions"
                                            add-member-name-prop="TimeDimension"
                                            :update-methods="scope"
                                    />
                                </ACol>
                            </ARow>


                            <ARow
                                    type="flex"
                                    justify="space-around"
                                    align="top"
                                    :gutter=24
                                    style="margin-bottom: 12px"
                            >
                                <ACol :span=24>
                                    <FilterGroup
                                            :members-prop="scope.filters"
                                            :available-members-prop="scope.availableDimensions.concat(scope.availableMeasures)"
                                            add-member-name-prop="Filter"
                                            :update-methods="scope"
                                    />
                                </ACol>
                            </ARow>


                            <ARow
                                    type="flex"
                                    justify="space-around"
                                    align="top"
                                    :gutter=24
                            >
                                <ACol :span=24>

                                    <a-button>
                                        <SelectChartType
                                                :chartType=scope.chartType
                                                :updateMethods=scope
                                        />
                                    </a-button>


                                    <ADivider type="vertical"/>

                                    <!--     <APopover
                                           content={
                                         <OrderGroup
                                           orderMembers={orderMembers}
                                           onReorder={updateOrder.reorder}
                                           onOrderChange={updateOrder.set}
                                         />
                                         }
                                         visible={isOrderPopoverVisible}
                                         placement="bottomLeft"
                                         trigger="click"
                                         onVisibleChange={visible => {
                                         if (!visible) {
                                         toggleOrderPopover(false);
                                         } else {
                                         if (orderMembers.length) {
                                         toggleOrderPopover(!isOrderPopoverVisible);
                                         }
                                         }
                                         }}
                                         >
                                         <AButton
                                           disabled={!orderMembers.length}
                                           icon={<SortAscendingOutlined />}
                                         >
                                         Order
                                         </AButton>
                                         </APopover>-->
                                </ACol>
                            </ARow>
                        </ACard>
                    </ACol>
                </ARow>


                <!--result-->
                <ARow type="flex" justify="space-around" align="top" :gutter=24>
                    <ACol :span=24>

                        <ACard
                                v-if="scope.isQueryPresent"
                                style="min-height: 420px">
                            <a-button
                                    key="button"
                                    type="primary"
                                    :loading="addingToDashboard"
                                    @click="setTitleModalVisible"
                                    slot="extra">
                                {{itemId ? "Update" : "Add to Dashboard"}}

                            </a-button>
                            Chart display
                            <ChartRenderer
                                    :query="scope.validatedQuery"
                                    :chartType="scope.chartType"
                                    :cubejsApi=cubejsApi
                            />
                        </ACard>

                        <h2
                                v-if="!scope.isQueryPresent"
                                style="text-align: center"
                        >
                            Choose a measure or dimension to get started
                        </h2>

                    </ACol>
                </ARow>
            </template>
        </query-builder>

    </div>
</template>

<script>
    /*
      import trackPng from '~/assets/img/baidu/track.png'
      */
    import cubejs from '@cubejs-client/core';
    import {QueryBuilder} from '~/selfmodule/cubejs-client-vue/src/index';
    import MemberGroup from "~/components/cube/QueryBuilder/MemberGroup";
    import TimeGroup from "~/components/cube/QueryBuilder/TimeGroup";
    import FilterGroup from "~/components/cube/QueryBuilder/FilterGroup";
    import SelectChartType from "~/components/cube/QueryBuilder/SelectChartType";
    import ChartRenderer from "~/components/cube/ChartRenderer";
    import TitleModal from "~/components/cube/TitleModal";
    import Action from "~/plugins/localStorageMutation";

    const cubejsApi = cubejs(
        '1c1ebc97f25e09b76fab81155adaddca99ad483e64b2fc0e0eb2a1574d988a5b81a5b37f20386b1ca8cf2e78d22b5c33757150adb99d5ea7b809368a721a194a',
        {apiUrl: 'http://localhost:4000/cubejs-api/v1'},
    );

    export default {
        name: "ExploreQueryBuilder",
        layout: 'empty',
        auth: 'false',
        components: {
            TitleModal,
            ChartRenderer, SelectChartType, FilterGroup, TimeGroup, MemberGroup, QueryBuilder
        },
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
            chartExtra: {
                type: Array,
                require: false,
                default: () => []
            },
            queryProp: {
                type: Object,
                require: false,
                default: () => {
                }
            },
        },
        data() {

            /* let query = {
               measures: [],
               timeDimensions: [
                 /!*  {
                     dimension: 'LineItems.createdAt',
                     granularity: 'month',
                   },*!/
               ],
               filters: [
                 /!*       {
                          "dimension": "Orders.status",
                          "operator": "contains",
                          "values": [
                            "1"
                          ]
                        }*!/
               ],
             };*/


            return {
                cubejsApi,
                query: this.queryProp,
                title: "demo",
                item: "",
                addingToDashboard: false,
                itemId: null,
                titleModalVisible: false,

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

            const params = new URLSearchParams(location.search);
            const itemId = params.get("itemId");
            this.itemId = itemId;
        },
        methods: {
            setTitleModalVisible() {
                this.titleModalVisible = true
            },
            submit({title}) {
                const item = {};
                let validatedQuery = this.$refs.queryBuilder.validatedQuery
                validatedQuery = {
                    "measures": ["LineItems.quantity"],
                    "timeDimensions": [{"dimension": "Orders.createdAt", "granularity": "day"}]
                };
                item.name = title;
                item.vizState = validatedQuery;
                /*   item.id = Action.Mutation.getNextId();
                   item.layout = {x:0,y:0,w:4,h:8};*/

                Action.Mutation.createDashboardItem(item);
                console.info("title--->", JSON.stringify(validatedQuery))
            },
            ceshi(str) {
                alert(str)
            },
            customLabel(a) {
                console.info("a.title", a.title)
                return a.title;
            },
            set(setMeasures, value) {
                setMeasures(value.map(e => e.name));
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
