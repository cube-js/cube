import Vue from 'vue';
import Chart from 'chart.js';
import VueChartkick from 'vue-chartkick';
import VueRouter from 'vue-router';
import VueApollo from 'vue-apollo';

import App from './App.vue';
import vuetify from './plugins/vuetify';
import Explore from './pages/explore/Explore.vue';
import Dashboard from './pages/dashboard/Dashboard.vue';
import apolloClient from './graphql/client';

Vue.use(VueApollo);

const apolloProvider = new VueApollo({
  defaultClient: apolloClient
});

import cubejs from '@cubejs-client/core';
const API_URL = 'https://ecom.cubecloudapp.dev';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1Ijp7fSwiaWF0IjoxNjExMjIyMjY4LCJleHAiOjE2MTM4MTQyNjh9.g7_sjO6qjQwblwHuVNnKfpjvwv9TBxyjZzWKtmRAlVI';
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`,
});

const router = new VueRouter({
  routes: [
    { path: '/explore', component: Explore, props: { cubejsApi } },
    { path: '/dashboard', component: Dashboard, props: { cubejsApi } },
  ]
})

Vue.config.productionTip = false;

Vue.use(VueChartkick, { adapter: Chart });
Vue.use(VueRouter);

new Vue({
  router,
  vuetify,
  apolloProvider,
  render: (h) => h(App),
}).$mount('#app');
