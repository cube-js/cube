<template>
  <v-container fluid class="text-center background pa-0">
    <grid-layout
      v-if="layout.length"
      :layout="layout"
      :col-num="12"
      :row-height="30"
      :vertical-compact="true"
      :use-css-transforms="true"
      @layout-updated="layoutUpdatedEvent"
    >
      <grid-item
        v-for="item in layout"
        :key="item.i"
        :x="layout.x"
        :y="layout.y"
        :w="layout.w"
        :h="layout.h"
        :i="layout.i"
      >
        {{item}}
        <!--<query-renderer :cubejsApi="cubejsApi" :query="JSON.parse(item.vizState).query">-->
          <!--<template #default="{ resultSet }">-->
            <!--<v-card :loading="!resultSet" class="px-4 py-2">-->
              <!--<v-card-title>{{item.name}}</v-card-title>-->
              <!--<template v-if="resultSet">-->
                <!--<line-chart v-if="item.type === 'line'" legend="bottom" :data="series(resultSet)"></line-chart>-->
                <!--<area-chart v-else-if="item.type === 'area'" legend="bottom" :data="series(resultSet)"></area-chart>-->
                <!--<pie-chart v-else-if="item.type === 'pie'" :data="pairs(resultSet)"></pie-chart>-->
                <!--<column-chart v-else-if="item.type === 'bar'" :data="seriesPairs(resultSet)"></column-chart>-->
                <!--<div v-else-if="item.type === 'number'">-->
                  <!--<div v-for="item in resultSet.series()" :key="item.key">-->
                    <!--{{ item.series[0].value }}-->
                  <!--</div>-->
                <!--</div>-->
                <!--<Table v-else :data="resultSet"></Table>-->
              <!--</template>-->

              <!--<v-card-actions>-->
                <!--<v-btn-->
                  <!--text-->
                  <!--@click="deleteDashboardItem(item.id)"-->
                <!--&gt;-->
                  <!--Delete-->
                <!--</v-btn>-->
              <!--</v-card-actions>-->

              <!--<template slot="progress">-->
                <!--<v-progress-linear-->
                  <!--color="deep-purple"-->
                  <!--height="10"-->
                  <!--indeterminate-->
                <!--&gt;</v-progress-linear>-->
              <!--</template>-->
            <!--</v-card>-->
          <!--</template>-->
        <!--</query-renderer>-->
      </grid-item>
    </grid-layout>
  </v-container>
</template>

<script>
  import gql from "graphql-tag";
  import { UPDATE_DASHBOARD_ITEM, DELETE_DASHBOARD_ITEM } from "../../graphql/mutations";
  import { QueryRenderer } from "@cubejs-client/vue";
  import Table from "../explore/components/Table";
  import VueGridLayout from "vue-grid-layout";

  export default {
    name: "Dashboard",
    props: {
      cubejsApi: {
        type: Object,
        required: true
      }
    },
    apollo: {
      dashboardItems: {
        query: gql`
          query {
            dashboardItems {
                  id
                  name
                  layout
                  vizState
                  type
                }
          }
        `,
        pollInterval: 1000,
        deep: true,
        update: data => {
          console.log(this);
          console.log(data);
          return data["dashboardItems"]
        }
      }
    },

    components: {
      QueryRenderer,
      Table,
      GridLayout: VueGridLayout.GridLayout,
      GridItem: VueGridLayout.GridItem
    },
    data() {
      return {
        snackbar: false,
        dashboardItems: [],
        layout: []
      };
    },
    methods: {
      series(resultSet) {
        const seriesNames = resultSet.seriesNames();
        const pivot = resultSet.chartPivot();
        const series = [];

        seriesNames.forEach((e) => {
          const data = pivot.map((p) => [p.x, p[e.key]]);
          series.push({ name: e.key, data });
        });
        return series;
      },
      pairs(resultSet) {
        return resultSet.series()[0].series.map((item) => [item.x, item.value]);
      },
      seriesPairs(resultSet) {
        return resultSet.series().map((seriesItem) => ({
          name: seriesItem.title,
          data: seriesItem.series.map((item) => [item.x, item.value])
        }));
      },
      async deleteDashboardItem(id) {
        const response = await this.$apollo.mutate({
          mutation: DELETE_DASHBOARD_ITEM,
          variables: {
            id
          }
        });
        console.log(">", response);
      },
      layoutUpdatedEvent(newLayout) {
        newLayout.forEach(l => {
          const item = this.dashboardItems.find(i => i.id.toString() === l.i);
          const toUpdate = JSON.stringify({
            x: l.x,
            y: l.y,
            w: l.w,
            h: l.h
          });
          if (item && toUpdate !== item.layout) {
            const newItem = {...item}
            const id = newItem.id;
            delete newItem['id'];
            delete newItem['__typename'];
            newItem.layout = toUpdate;
            this.$apollo.mutate({
              // Query
              mutation: UPDATE_DASHBOARD_ITEM,
              // Parameters
              variables: {
                id,
                input: newItem
              },
            }).then((data) => {
              // Result
              console.log(data)
            }).catch((error) => {
              // Error
              console.error(error)
            })
          }
        });
      }
    },
  };
</script>

<style scopped>

</style>
