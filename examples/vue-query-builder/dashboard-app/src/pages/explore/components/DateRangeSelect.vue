<template>
  <v-select
    label="Date Range"
    outlined
    hide-details
    clearable
    :value="timeDimensions[0] && timeDimensions[0].dateRange"
    :items="dateRangeItems"
    @change="changeHandler"
  />
</template>

<script>
export default {
  name: 'DateRangeSelect',
  props: {
    timeDimensions: Array,
  },
  data() {
    return {
      dateRangeItems: [
        'Today',
        'Yesterday',
        'This week',
        'This month',
        'This quarter',
        'This year',
        'Last 30 days',
        'Last year',
      ],
    };
  },
  methods: {
    changeHandler(value) {
      this.$emit('change', [
        {
          dimension: this.timeDimensions[0].dimension.name,
          granularity: this.timeDimensions[0].granularity,
          ...(value ? { dateRange: value } : null),
        },
      ]);
    },
  },
};
</script>
