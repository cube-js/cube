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
              <line-chart legend="bottom" :data="series(resultSet)"></line-chart>
            </template>

            <v-card-text>
              <v-row
                align="center"
                class="mx-0"
              >
                <div class="grey--text">
                  {{JSON.stringify(item)}}
                </div>
              </v-row>
            </v-card-text>

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
                }
          }
        `,
        pollInterval: 1000,
        deep: true,
        update: data => data["dashboardItems"]
      }
    },

    components: {
      QueryRenderer
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
