<template>
  <v-dialog v-model="dialog" max-width="480">
    <template #activator="{ on, attrs }">
      <v-btn v-bind="attrs" v-on="on" :disabled="!pivotConfig || disabled"> Pivot </v-btn>
    </template>

    <v-card>
      <v-card-title>Pivot config</v-card-title>
      <v-card-text>
        <div class="container">
          <div>
            <div class="axis-name">x</div>
            <draggable id="x" class="list-group" group="pivot" v-model="draggableX">
              <div class="list-group-item" v-for="member in draggableX" :key="member">
                <v-icon small>mdi-arrow-all</v-icon>

                <span>{{ member }}</span>
              </div>
            </draggable>
          </div>

          <div><v-divider vertical class="divider" /></div>

          <div>
            <div class="axis-name">y</div>
            <draggable id="y" class="list-group" group="pivot" v-model="draggableY">
              <div class="list-group-item" v-for="member in draggableY" :key="member">
                <v-icon small>mdi-arrow-all</v-icon>

                <span>{{ member }}</span>
              </div>
            </draggable>
          </div>
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
    disabled: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      dialog: false,
    };
  },
  computed: {
    draggableX: {
      get() {
        return this.pivotConfig.x;
      },
      set(value) {
        this.$emit('move', {
          x: value,
        });
      },
    },
    draggableY: {
      get() {
        return this.pivotConfig.y;
      },
      set(value) {
        this.$emit('move', {
          y: value,
        });
      },
    },
  },
  methods: {
    checkMove(event) {
      // Make sure the `measures` is always the last item on axis
      const sourceAxis = event.from.id;
      const destinationAxis = event.to.id;
      let maxIndexOnAxis = this.pivotConfig[destinationAxis].length - 1;

      if (sourceAxis === destinationAxis) {
        maxIndexOnAxis--;
      }

      if (event.draggedContext.element === 'measures') {
        if (event.draggedContext.futureIndex <= maxIndexOnAxis) {
          return false;
        }
      } else {
        const { length } = this.pivotConfig[destinationAxis];
        if (this.pivotConfig[destinationAxis][length - 1] === 'measures') {
          if (event.draggedContext.futureIndex > maxIndexOnAxis) {
            return false;
          }
        }
      }

      return true;
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

.list-group-item > i {
  margin-right: 8px;
}

.divider {
  margin: 0 12px;
}
</style>
