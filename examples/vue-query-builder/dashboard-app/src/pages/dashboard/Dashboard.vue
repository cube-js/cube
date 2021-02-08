<template>
  <v-container fluid class="text-center background pa-0">
    <v-snackbar
      color="success"
      top
      v-model="snackbar"
    >
      {{ text }}

      <template v-slot:action="{ attrs }">
        <v-btn
          text
          v-bind="attrs"
          @click="snackbar = false"
        >
          Close
        </v-btn>
      </template>
    </v-snackbar>
    The Dashboard
    <grid-layout
      :layout.sync="layout"
      :col-num="12"
      :row-height="430"
      :is-draggable="true"
      :is-resizable="true"
      :is-mirrored="false"
      :vertical-compact="true"
      :margin="[10, 10]"
      :use-css-transforms="true"
      :preventCollision="false"
      :responsive="true"
    >
        <grid-item
                   v-for="(item, index) in dashboardItems"
                   :key="item.id"
                   :x="layout[index].x"
                   :y="layout[index].y"
                   :w="layout[index].w"
                   :h="layout[index].h"
                   :i="layout[index].i">
          <query-renderer :cubejsApi="cubejsApi" :query="JSON.parse(item.vizState).query">
            <template #default="{ resultSet }">
              <v-card :loading="!resultSet" class="px-4 py-2">
                <v-card-title>{{item.name}}</v-card-title>
                <template v-if="resultSet">
                  <line-chart v-if="item.type === 'line'" legend="bottom" :data="series(resultSet)"></line-chart>
                  <area-chart v-else-if="item.type === 'area'" legend="bottom" :data="series(resultSet)"></area-chart>
                  <pie-chart v-else-if="item.type === 'pie'" :data="pairs(resultSet)"></pie-chart>
                  <column-chart v-else-if="item.type === 'bar'" :data="seriesPairs(resultSet)"></column-chart>
                  <div v-else-if="item.type === 'number'">
                    <div v-for="item in resultSet.series()" :key="item.key">
                      {{ item.series[0].value }}
                    </div>
                  </div>
                  <Table v-else :data="resultSet"></Table>
                </template>

                <v-card-actions>
                  <v-btn
                    text
                    @click="deleteDashboardItem(item.id)"
                  >
                    Delete
                  </v-btn>
                </v-card-actions>

                <template slot="progress">
                  <v-progress-linear
                    color="deep-purple"
                    height="10"
                    indeterminate
                  ></v-progress-linear>
                </template>
              </v-card>
            </template>
          </query-renderer>
        </grid-item>
    </grid-layout>
  </v-container>
</template>

<script>
  import gql from "graphql-tag";
  import { QueryRenderer } from "@cubejs-client/vue";
  import Table from '../explore/components/Table';
  import VueGridLayout from 'vue-grid-layout';

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
        update: data => data["dashboardItems"]
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
        text: '',
        layout: [
          {"x":0,"y":0,"w":6,"h":1,"i":"0"},
          {"x":6,"y":0,"w":6,"h":1,"i":"1"},
        ],
        index: 0,
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
          mutation: gql`
          mutation($id: String!) {
            deleteDashboardItem(id: $id) {
              id
            }
          }
        `,
          variables: {
            id
          },
        });
        this.openSnackBar('Deleted');
        console.log('>', response);
      },
      openSnackBar(text) {
        this.text = text;
        this.snackbar = true;
        setTimeout(() => {
          this.text = '';
          this.snackbar = false;
        }, 2000);
      },
      addItem: function () {
        // Add a new item. It must have a unique key!
        this.layout.push({
          x: (this.layout.length * 2) % (this.colNum || 12),
          y: this.layout.length + (this.colNum || 12), // puts it at the bottom
          w: 2,
          h: 2,
          i: this.index,
        });
        // Increment the counter to ensure key is always unique.
        this.index++;
      },
      removeItem: function (val) {
        const index = this.layout.map(item => item.i).indexOf(val);
        this.layout.splice(index, 1);
      },
    }
  };
</script>

<style scopped>

</style>
