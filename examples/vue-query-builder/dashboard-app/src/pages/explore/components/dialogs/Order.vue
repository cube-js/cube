<template>
  <v-dialog v-model="dialog" max-width="400">
    <template #activator="{ on, attrs }">
      <v-btn v-bind="attrs" v-on="on" :disabled="disabled"> Order </v-btn>
    </template>

    <v-card>
      <v-card-title>Order</v-card-title>
      <v-card-text class="container">
        <draggable class="list-group" v-model="list" @end="handleDragEnd">
          <div v-for="member in list" :key="member.id" class="order-member">
            <div class="order-member-name">
              <v-icon small>mdi-arrow-all</v-icon>

              <span>{{ member.title }}</span>
            </div>

            <v-btn-toggle borderless :value="member.order" @change="(value) => $emit('orderChange', member.id, value)">
              <v-btn small value="asc"> ASC </v-btn>
              <v-btn small value="desc"> DESC </v-btn>
              <v-btn small value="none"> NONE </v-btn>
            </v-btn-toggle>
          </div>
        </draggable>
      </v-card-text>
    </v-card>
  </v-dialog>
</template>

<script>
import draggable from 'vuedraggable';

export default {
  name: 'Order',
  components: {
    draggable,
  },
  props: {
    orderMembers: {
      type: Array,
      required: true,
    },
    disabled: {
      type: Boolean,
      default: false,
    },
  },

  computed: {
    list: {
      get() {
        return this.orderMembers;
      },
      set(value) {
        return value;
      }
    },
  },

  data() {
    return {
      dialog: false,
    };
  },

  methods: {
    handleDragEnd(event) {
      this.$emit('reorder', event.oldIndex, event.newIndex);
    },
  },
};
</script>

<style scoped>
.order-member {
  display: flex;
  justify-content: space-between;
  width: 100%;
  margin: 16px 0;
}

.order-member-name {
  cursor: grab;
}

.order-member-name > i {
  margin-right: 8px;
}
</style>
