<template>
  <v-dialog v-model="dialog" max-width="480">
    <template v-slot:activator="{ on, attrs }">
      <v-btn v-bind="attrs" v-on="on" :disabled="!pivotConfig"> Pivot </v-btn>
    </template>

    <v-card>
      <v-card-title>Pivot config</v-card-title>
      <v-card-text>
        <div class="container">
          <div>
            <div class="axis-name">x</div>
            <draggable id="x" class="list-group" group="pivot" :list="pivotConfig.x" :move="checkMove">
              <div class="list-group-item" v-for="member in pivotConfig.x" :key="member">
                <v-icon small>mdi-arrow-all</v-icon>

                <span>{{ member }}</span>
              </div>
            </draggable>
          </div>

          <div><v-divider vertical class="divider" /></div>

          <div>
            <div class="axis-name">y</div>
            <draggable id="y" class="list-group" group="pivot" :list="pivotConfig.y" :move="checkMove">
              <div class="list-group-item" v-for="member in pivotConfig.y" :key="member">
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

const items = new Map();

function onMoveEnd(axis, data, callback) {
  const { added, removed } = data;
  const id = added?.element || removed?.element;
  const from = removed !== undefined ? removed : null;
  const to = added !== undefined ? added : null;
  const isSourceEvent = Boolean(from);

  const item = items.get(id) || {};
  const nextItem = {
    sourceAxis: isSourceEvent ? axis : item.sourceAxis,
    destinationAxis: !isSourceEvent ? axis : item.destinationAxis,
    sourceIndex: item.sourceIndex != null ? item.sourceIndex : from?.oldIndex,
    destinationIndex: item.destinationIndex != null ? item.destinationIndex : to?.newIndex,
  };
  items.set(id, nextItem);

  if (nextItem.sourceAxis && nextItem.destinationAxis) {
    callback({
      element: id,
      ...nextItem,
    });
    items.delete(id);
  }
}

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
    // onMove: {
    //   type: Function,
    //   required: true,
    // },
  },
  data() {
    return {
      dialog: false,
    };
  },
  methods: {
    checkMove(event) {
      // todo: move to core utils
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
