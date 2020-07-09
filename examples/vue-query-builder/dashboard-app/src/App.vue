<template>
  <v-app>
    <v-app-bar
      app
      color="#43436B"
      dark
    >
      <div class="d-flex align-center">
        <v-img
          alt="Vuetify Logo"
          class="shrink mr-2"
          contain
          src="https://material-ui-dashboard.cubecloudapp.dev/images/logo/logo.svg"
          transition="scale-transition"
        />
      </div>
    </v-app-bar>

    <v-main>
      <HelloWorld/>
    </v-main>
  </v-app>
</template>

<script>
import HelloWorld from './components/HelloWorld'

export default {
  name: 'App',

  components: {
    HelloWorld
  }
}
</script>
