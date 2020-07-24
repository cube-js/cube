module.exports = (api, options, rootOptions) => {
  // change `package.json`
  api.extendPackage({
    scripts: {
      start: "vue-cli-service serve"
    },
    dependencies: {
      "@cubejs-client/core": "^0.19.48",
      "@cubejs-client/vue": "^0.19.48",
      "ant-design-vue": "^1.6.3",
      "core-js": "^3.6.5",
      "echarts": "^4.8.0",
      "vue": "^2.6.11",
      "vue-echarts": "^5.0.0-beta.0",
      "vue-grid-layout": "^2.3.7"
    },
  })
  api.render('../template')
}
