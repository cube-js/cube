<template>
  <v-dialog v-model="dialog" max-width="480">
    <template v-slot:activator="{ on, attrs }">
      <v-btn v-bind="attrs" v-on="on"> Limit </v-btn>
    </template>

    <v-card>
      <v-card-title>Limit</v-card-title>
      <v-card-text class="container">
        <v-text-field
          type="number"
          label="Limit"
          hide-details="auto"
          :value="limit"
          @change="(limit) => $emit('update', Number(limit))"
        ></v-text-field>
      </v-card-text>
    </v-card>
  </v-dialog>
</template>

<script>
import draggable from 'vuedraggable';

export default {
  name: 'Limit',
  components: {
    draggable,
  },
  props: {
    limit: {
      type: Number,
      required: true,
    },
  },
  data() {
    return {
      dialog: false,
    };
  },
};
</script>

<style scoped>
.container {
  display: grid;
  grid-template-columns: 1fr auto 1fr;
}

.axis-name {
  font-weight: bold;
  text-align: center;
}

.list-group {
  height: 100%;
}

.list-group-item {
  cursor: grab;
}

.divider {
  margin: 0 12px;
}
</style>
