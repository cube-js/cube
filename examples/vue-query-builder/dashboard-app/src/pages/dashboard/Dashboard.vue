<template>
  <v-container fluid class="text-center background pa-0">
    The Dashboard {{test}}
  </v-container>
</template>

<script>
import gql from 'graphql-tag'

export default {
  name: 'Explore',
  
  apollo: {
    test: gql`
      query {
        test  
      }
    `
  },

  components: {
  },
  data() {
    return {

    };
  },
  methods: {

  },
};
</script>

<style scopped>

</style>
