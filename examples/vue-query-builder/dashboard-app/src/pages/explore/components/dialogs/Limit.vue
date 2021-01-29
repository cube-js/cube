<template>
  <v-dialog v-model="dialog" max-width="480">
    <template #activator="{ on, attrs }">
      <v-btn v-bind="attrs" v-on="on" :disabled="disabled"> Limit </v-btn>
    </template>

    <v-card>
      <v-card-title>Limit</v-card-title>
      <v-card-text>
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
    disabled: {
      type: Boolean,
      default: false
    },
  },
  data() {
    return {
      dialog: false,
    };
  },
};
</script>
