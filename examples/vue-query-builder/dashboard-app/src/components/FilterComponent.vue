<template>
  <v-row>
    <v-col cols="12" md="2" v-if="showFilters">
      <v-select
        label="Select Dimension or Measure"
        outlined
        hide-details
        v-model="select"
        :items="[...measures, ...dimensions]"
      />
    </v-col>

    <v-col cols="12" md="2" v-if="select">
      <v-select
        label="Select action"
        v-model="operator"
        item-text="text"
        item-value="value"
        outlined
        hide-details
        :items="actionItems"
      />
    </v-col>

    <v-col cols="12" md="2" v-if="operator">
      <v-text-field label="Value" outlined hide-details v-model="value"></v-text-field>
    </v-col>

    <v-col cols="12" class="pa-0">
      <div>
        <template>
          <v-col cols="2" class="mb-2" v-for="(filter, index) in filters" :key="index">
            <div class="mx-auto filter-card">
              <v-list-item>
                <v-list-item-content>
                  <v-list-item-title>
                    {{ filter['member']['title'] }}
                    <v-icon @click="removeFilter(index, filter)">mdi-filter-remove-outline</v-icon>
                  </v-list-item-title>
                </v-list-item-content>
              </v-list-item>
            </div>
          </v-col>
        </template>
      </div>
    </v-col>

    <v-col cols="12" class="d-flex flex-start pb-0 mt-2">
      <v-btn v-if="select" color="primary" @click="save"> Save filter </v-btn>
      <v-btn v-else color="primary" outlined @click="save"> Add filter </v-btn>
    </v-col>
  </v-row>
</template>

<script>
export default {
  name: 'FilterComponent',
  props: ['filters', 'dimensions', 'measures', 'setFilters'],
  data: () => ({
    dialog: false,
    select: '',
    operator: '',
    value: '',
    actionItems: [
      {
        text: 'equals',
        value: 'equals',
      },
      {
        text: 'does not equal',
        value: 'notEquals',
      },
      {
        text: 'is set',
        value: 'set',
      },
      {
        text: 'is not set',
        value: 'notSet',
      },
      {
        text: '>',
        value: 'gt',
      },
      {
        text: '>=',
        value: 'gte',
      },
      {
        text: '<',
        value: 'lt',
      },
      {
        text: '<=',
        value: 'lte',
      },
    ],
    showFilters: false,
  }),
  methods: {
    save() {
      if (!this.showFilters) {
        this.showFilters = true;
        return true;
      }
      this.dialog = false;
      let newFilters = [
        ...this.filters,
        {
          member: this.select,
          operator: this.operator,
          values: [this.value],
        },
      ];
      this.setFilters(newFilters);
      this.clearFilter();
    },
    removeFilter(index) {
      this.filters.splice(index, 1);
      let newFilters = [...this.filters];
      this.setFilters(newFilters);
    },
    clearFilter() {
      this.select = '';
      this.operator = '';
      this.value = '';
    },
  },
};
</script>
