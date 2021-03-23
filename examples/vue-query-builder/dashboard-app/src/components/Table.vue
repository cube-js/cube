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
      const data = this.data.tablePivot();
      return data.map((key) => {
        return key;
      });
    },
  },
};
</script>
