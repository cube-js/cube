<template>
  <v-dialog v-model="dialog" max-width="480">
    <template #activator="{ on, attrs }">
      <v-btn v-bind="attrs" v-on="on" color="primary">Add to Dashboard</v-btn>
    </template>

    <v-card>
      <v-form @submit="handleSave">
        <v-card-title>Add to dashboard</v-card-title>

        <v-card-text>
          <v-text-field
            label="Chart name"
            hide-details="auto"
            v-model="chartName"
          ></v-text-field>
        </v-card-text>

        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn color="blue darken-1" text @click="dialog = false"> Close </v-btn>
          <v-btn type="submit" color="blue darken-1" text @click="handleSave"> Save </v-btn>
        </v-card-actions>
      </v-form>
    </v-card>
  </v-dialog>
</template>

<script>
export default {
  name: 'AddToDashboard',
  data() {
    return {
      dialog: false,
      chartName: 'New Chart',
    };
  },
  methods: {
    handleSave(event) {
      event.preventDefault();
      this.dialog = false;
      this.$emit('onSave', this.chartName);
    },
  },
};
</script>
