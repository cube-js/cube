<template>
  <v-data-table
    :headers="headers"
    :items="desserts"
    :items-per-page="5"
    class="elevation-1"
  ></v-data-table>
</template>

<script>
  export default {
    name: 'Table',
    props: ['data'],
    mounted() {
    },
    data: () => ({
    }),
    computed: {
      headers() {
        let data = this.data.tablePivot();
        let headers = Object.keys(data[0]).map(key => {
          return {
            text: key,
            align: 'start',
            sortable: true,
            value: key,
          }
        })
        return headers
      },
      desserts() {
        let data = this.data.tablePivot();
        let desserts = data.map(key => {
          return key
        })
        return desserts
      }
    }
  }
</script>
