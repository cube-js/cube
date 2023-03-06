<template>
  <v-app>
    <v-toolbar app color="#43436B" dark class="px-4">
      <v-tabs>
        <v-tab to="/explore">Explore</v-tab>
        <v-tab to="/dashboard">Dashboard</v-tab>
      </v-tabs>
    </v-toolbar>
    <v-main>
      <router-view></router-view>
    </v-main>
  </v-app>
</template>
<script>
export default {
  name: 'App',

  components: {},
};
</script>

<style scoped>
.custom-btn {
  display: flex;
  align-items: center;
  padding: 6px 12px;
  background: rgba(243, 243, 251, 0.1);
  border-radius: 4px;
  text-decoration: none;
  font-style: normal;
  font-weight: normal;
  font-size: 16px;
  line-height: 26px;
  letter-spacing: 0.02em;
  font-feature-settings: 'ss03' on, 'ss04' on;
  color: #ffffff;
}
.custom-btn:hover {
  background: rgba(243, 243, 251, 0.2);
}
</style>
