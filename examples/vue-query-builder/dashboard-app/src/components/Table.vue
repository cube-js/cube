<template>
  <v-data-table v-if="resultSet" :headers="headers" :items="items" :items-per-page="50" class="elevation-1"></v-data-table>
</template>

<script>
import { ResultSet } from '@cubejs-client/core';

export default {
  name: 'Table',
  props: {
    resultSet: {
      type: ResultSet,
      required: true,
    },
  },
  mounted() {},
  data: () => ({}),
  computed: {
    headers() {
      const data = this.resultSet.tablePivot();
      return Object.keys(data[0]).map((key) => {
        return {
          text: key,
          align: 'start',
          sortable: true,
          value: key,
        };
      });
    },
    items() {
      const data = this.resultSet.tablePivot();
      return data.map((key) => {
        return key;
      });
    },
  },
};
</script>
