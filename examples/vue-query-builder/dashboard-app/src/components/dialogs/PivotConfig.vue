<template>
  <v-dialog v-model="dialog" max-width="480">
    <template v-slot:activator="{ on, attrs }">
      <v-btn v-bind="attrs" v-on="on"> Pivot </v-btn>
    </template>

    <v-card>
      <v-card-title>Pivot config</v-card-title>
      <v-card-text class="container">
        <div>
          <div class="axis-name">x</div>
          <draggable class="list-group" :list="pivotConfig.x" group="pivot" @change="log">
            <div class="list-group-item" v-for="(member) in pivotConfig.x" :key="member">
              {{ member }}
            </div>
          </draggable>
        </div>

        <div><v-divider vertical class="divider" /></div>

        <div>
          <div class="axis-name">y</div>
          <draggable class="list-group" :list="pivotConfig.y" group="pivot" @change="log">
            <div class="list-group-item" v-for="(member) in pivotConfig.y" :key="member">
              {{ member }}
            </div>
          </draggable>
        </div>
      </v-card-text>
    </v-card>
  </v-dialog>
</template>

<script>
import draggable from 'vuedraggable';

export default {
  name: 'PivotConfig',
  components: {
    draggable,
  },
  props: {
    pivotConfig: {
      type: Object,
      required: true,
    },
  },
  data() {
    return {
      dialog: false,
    };
  },
  methods: {
    log(value) {
      console.log('logger', value);
    },
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
