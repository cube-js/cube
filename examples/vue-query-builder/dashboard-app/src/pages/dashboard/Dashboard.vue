<template>
  <v-container fluid class="text-center background pa-0">
    <Grid v-if="dashboardItems.length" :cubejs-api="cubejsApi" :dashboardItems="dashboardItems" />
  </v-container>
</template>

<script>
import gql from 'graphql-tag';
import Grid from './components/Grid';

export default {
  name: 'Dashboard',
  props: {
    cubejsApi: {
      type: Object,
      required: true,
    },
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
      update: (data) => {
        return data['dashboardItems'];
      },
    },
  },

  components: {
    Grid,
  },
  data() {
    return {
      dashboardItems: [],
    };
  },
};
</script>
