import Vue from 'vue'
import App from './App.vue'
import vuetify from './plugins/vuetify'
import Chart from 'chart.js'
import VueChartkick from 'vue-chartkick'

Vue.config.productionTip = false
Vue.use(VueChartkick, { adapter: Chart })

new Vue({
  vuetify,
  render: h => h(App)
}).$mount('#app')
