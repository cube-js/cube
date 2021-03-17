<template>
  <v-select
    label="Time Dimensions"
    outlined
    hide-details
    :value="timeDimensions[0] && timeDimensions[0].dimension.name"
    :items="availableTimeDimensions.map((i) => i.name)"
    clearable
    @change="handleTimeChange"
  />
</template>

<script>
  export default {
    props: ["timeDimensions", "availableTimeDimensions"],
    name: "TimeDimensionSelect.vue",
    methods: {
      handleTimeChange(value) {
        const [selectedTd = {}] = this.timeDimensions;
        const td = this.availableTimeDimensions.find(({ name }) => name === value);

        if (!td) {
          this.$emit('change', []);
          return;
        }
        this.$emit('change', [
          {
            dimension: td.name,
            granularity: selectedTd.granularity || 'day',
            dateRange: selectedTd.dateRange,
          },
        ]);
      }
    }
  };
</script>

<style scoped>

</style>
