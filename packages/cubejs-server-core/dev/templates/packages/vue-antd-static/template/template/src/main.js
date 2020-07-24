import Vue from 'vue'
import App from './App.vue'

Vue.config.productionTip = false


/*ant-design-vue'*/
import Antd from 'ant-design-vue'
import 'ant-design-vue/dist/antd.css'
Vue.use(Antd)

import ECharts from 'vue-echarts'
Vue.component('v-chart', ECharts)


new Vue({
  render: h => h(App),
}).$mount('#app')
