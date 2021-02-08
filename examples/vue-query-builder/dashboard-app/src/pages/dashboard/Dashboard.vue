<template>
  <v-container fluid class="text-center background pa-0">
    <v-snackbar
      color="success"
      top
      v-model="snackbar"
      :vertical="vertical"
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
    <div class="px-12 my-4" v-for="item in dashboardItems" :key="item.id">
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
    </div>
  </v-container>
</template>

<script>
  import gql from "graphql-tag";
  import { QueryRenderer } from "@cubejs-client/vue";
  import Table from '../explore/components/Table';

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
      Table
    },
    data() {
      return {
        snackbar: false,
        text: ''
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
      }
    }
  };
</script>

<style scopped>

</style>
