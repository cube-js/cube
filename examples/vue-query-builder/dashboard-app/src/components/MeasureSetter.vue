<template>
  <v-select
    multiple
    v-model="state"
    :item-text="items.map(i => (i.name))"
    :item-value="items.map(i => (i.name))"
    :items="items.map(i => (i.name))"
    label="Select a measure"
  ></v-select>
</template>
<script>
export default {
  props: ['items'],
  mounted () {
  },
  data: () => ({
    state: []
  })
}
</script>
