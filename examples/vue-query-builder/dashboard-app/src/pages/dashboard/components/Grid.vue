<template>
  <grid-layout
    v-if="layout.length"
    :layout="layout"
    :col-num="colNum"
    :row-height="20"
    :vertical-compact="true"
    :use-css-transforms="true"
    @layout-updated="layoutUpdatedEvent"
  >
    <grid-item v-for="item in layout" :key="item.i" :x="item.x" :y="item.y" :w="item.w" :h="item.h" :i="item.i">
      <query-renderer class="height-100" :cubejsApi="cubejsApi" :query="getQueryById(item.i)">
        <template #default="{ resultSet }">
          <v-card :loading="!resultSet" class="px-4 py-2" height="100%">
            <v-card-title>{{ item.value.name }}</v-card-title>

            <template v-if="resultSet">
              <ChartRenderer :result-set="resultSet" :chart-type="item.value.type"></ChartRenderer>
            </template>

            <v-card-actions>
              <v-btn plain color="error" @click="deleteDashboardItem(item.i)"> Delete </v-btn>
            </v-card-actions>

            <template slot="progress">
              <v-progress-linear color="deep-purple" height="10" indeterminate></v-progress-linear>
            </template>
          </v-card>
        </template>
      </query-renderer>
    </grid-item>
  </grid-layout>
</template>

<script>
import { UPDATE_DASHBOARD_ITEM, DELETE_DASHBOARD_ITEM } from '@/graphql/mutations';
import { QueryRenderer } from '@cubejs-client/vue';
import VueGridLayout from 'vue-grid-layout';

import ChartRenderer from '@/components/ChartRenderer';

export default {
  name: 'Grid',
  props: {
    cubejsApi: {
      type: Object,
      required: true,
    },
    dashboardItems: {
      type: Array,
      required: true,
    },
  },

  components: {
    ChartRenderer,
    QueryRenderer,
    GridLayout: VueGridLayout.GridLayout,
    GridItem: VueGridLayout.GridItem,
  },
  data() {
    return {
      layout: [],
      colNum: 12,
    };
  },
  watch: {
    dashboardItems: {
      deep: true,
      handler() {
        this.updateLayout();
      },
    },
  },
  beforeMount() {
    this.updateLayout();
  },
  methods: {
    updateLayout() {
      this.layout = this.dashboardItems.map((item) => {
        let layout = JSON.parse(item.layout);
        // check empty obj
        if (Object.keys(layout).length === 0) {
          // add new
          layout = {
            x: (this.dashboardItems.length * 2) % (this.colNum || 12),
            y: this.dashboardItems.length + (this.colNum || 12), // puts it at the bottom
            w: 6,
            h: 20,
            i: item.id,
          };
        }
        return { ...layout, i: +item.id, value: item };
      });
    },
    getQueryById(id) {
      let item = this.dashboardItems.filter((item) => +item.id === id);
      return JSON.parse(item[0].vizState).query;
    },
    async deleteDashboardItem(id) {
      await this.$apollo.mutate({
        mutation: DELETE_DASHBOARD_ITEM,
        variables: {
          id: id.toString(),
        },
      });
    },
    layoutUpdatedEvent(newLayout) {
      newLayout.forEach(async (l) => {
        const item = this.dashboardItems.find((i) => {
          return +i.id === l.i;
        });
        const toUpdate = JSON.stringify({
          x: l.x,
          y: l.y,
          w: l.w,
          h: l.h,
        });

        if (item && toUpdate !== item.layout) {
          const newItem = { ...item };
          const id = newItem.id;
          delete newItem['id'];
          delete newItem['__typename'];
          newItem.layout = toUpdate;

          await this.$apollo.mutate({
            mutation: UPDATE_DASHBOARD_ITEM,
            variables: {
              id,
              input: newItem,
            },
          });
        }
      });
    },
  },
};
</script>

<style>
.height-100 {
  height: 100%;
  max-height: 100%;
  min-height: 100%;
  overflow-y: hidden;
}
</style>
