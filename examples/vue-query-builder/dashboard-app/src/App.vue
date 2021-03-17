<template>
  <v-app>
    <v-app-bar app color="#43436B" dark class="px-4">
      <div class="d-flex align-center">
        <v-img
          alt="Vuetify Logo"
          class="shrink mr-2"
          contain
          src="https://cube.dev/downloads/logo-full.svg"
          transition="scale-transition"
        />
      </div>

        <v-tabs align-with-title>
          <v-tab to="/explore">Explore</v-tab>
          <v-tab to="/dashboard">Dashboard</v-tab>
        </v-tabs>

      <v-spacer></v-spacer>

      <v-hover v-slot:default="{ hover }">
        <a
          class="custom-btn mr-4"
          href="https://github.com/cube-js/cube.js/tree/master/examples/vue-query-builder"
          target="_blank"
        >
          <v-img
            v-if="!hover"
            alt="GitHub Logo"
            src="./assets/github.svg"
            class="mr-2"
            width="20px"
            height="20px"
            transition="scale-transition"
          />
          <v-img
            v-else
            alt="GitHub Logo"
            src="./assets/githubWhite.svg"
            class="mr-2"
            width="20px"
            height="20px"
            transition="scale-transition"
          />
          <button type="button">GitHub</button>
        </a>
      </v-hover>
      <v-hover v-slot:default="{ hover }">
        <a class="custom-btn" href="https://slack.cube.dev/" target="_blank">
          <v-img
            v-if="!hover"
            alt="Slack Logo"
            src="./assets/slack.svg"
            class="mr-2"
            width="20px"
            height="20px"
            transition="scale-transition"
          />
          <v-img
            v-else
            alt="Slack Logo"
            src="./assets/slackWhite.svg"
            class="mr-2"
            width="20px"
            height="20px"
            transition="scale-transition"
          />
          <button type="button">Slack</button>
        </a>
      </v-hover>
    </v-app-bar>

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
