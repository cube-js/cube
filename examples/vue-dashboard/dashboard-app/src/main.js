import Vue from "vue";
import { Laue } from 'laue';
import App from "./App.vue";

Vue.use(Laue);

Vue.config.productionTip = false;

new Vue({
  render: h => h(App)
}).$mount("#app");
