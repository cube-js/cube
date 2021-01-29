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

const router = new VueRouter({
  routes: [
    { path: '/explore', component: Explore },
    { path: '/dashboard', component: Dashboard },
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
