<template>
  <v-data-table :headers="headers" :items="items" :items-per-page="50" class="elevation-1"></v-data-table>
</template>

<script>
export default {
  name: 'Table',
  props: ['data'],
  mounted() {},
  data: () => ({}),
  computed: {
    headers() {
      const data = this.data.tablePivot();
      const headers = Object.keys(data[0]).map((key) => {
        return {
          text: key,
          align: 'start',
          sortable: true,
          value: key,
        };
      });
      return headers;
    },
    items() {
      const data = this.data.tablePivot();
      const items = data.map((key) => {
        return key;
      });
      return items;
    },
  },
};
</script>
