var plugins = [{
      plugin: require('/Users/lonya/Desktop/cube.js/guides/angular-dashboard-with-material-ui/node_modules/gatsby-plugin-react-helmet/gatsby-ssr'),
      options: {"plugins":[]},
    },{
      plugin: require('/Users/lonya/Desktop/cube.js/guides/angular-dashboard-with-material-ui/node_modules/gatsby-remark-autolink-headers/gatsby-ssr'),
      options: {"plugins":[]},
    },{
      plugin: require('/Users/lonya/Desktop/cube.js/guides/angular-dashboard-with-material-ui/node_modules/gatsby-plugin-google-analytics/gatsby-ssr'),
      options: {"plugins":[],"trackingId":"UA-70480064-3"},
    },{
      plugin: require('/Users/lonya/Desktop/cube.js/guides/angular-dashboard-with-material-ui/node_modules/gatsby-plugin-snowplow-tracker/gatsby-ssr'),
      options: {"plugins":[],"snippetHost":"//d1fc8wv8zag5ca.cloudfront.net","snippetVersion":"2.10.2","namespace":"scalacoll","collectorUri":"snowplow-collector.cube.dev","config":{"appId":"d3-dashboard-guide"}},
    },{
      plugin: require('/Users/lonya/Desktop/cube.js/guides/angular-dashboard-with-material-ui/node_modules/gatsby-plugin-sitemap/gatsby-ssr'),
      options: {"plugins":[]},
    },{
      plugin: require('/Users/lonya/Desktop/cube.js/guides/angular-dashboard-with-material-ui/node_modules/gatsby-plugin-styled-components/gatsby-ssr'),
      options: {"plugins":[]},
    },{
      plugin: require('/Users/lonya/Desktop/cube.js/guides/angular-dashboard-with-material-ui/node_modules/gatsby-plugin-manifest/gatsby-ssr'),
      options: {"plugins":[],"name":"Material UI Dashboard with Angular","short_name":"Material UI Dashboard with Angular","description":"How to build Angular Material Dashboard with Cube.js.","start_url":"","background_color":"#e0e0e0","theme_color":"#c62828","display":"minimal-ui","icon":"/Users/lonya/Desktop/cube.js/guides/guides-base/static/logos/icon.png","cache_busting_mode":"query","include_favicon":true,"legacy":true,"theme_color_in_head":true,"cacheDigest":"c79b6ee44cd5a2d74a48cb83170cce6f"},
    },{
      plugin: require('/Users/lonya/Desktop/cube.js/guides/angular-dashboard-with-material-ui/node_modules/gatsby-plugin-offline/gatsby-ssr'),
      options: {"plugins":[]},
    }]
// During bootstrap, we write requires at top of this file which looks like:
// var plugins = [
//   {
//     plugin: require("/path/to/plugin1/gatsby-ssr.js"),
//     options: { ... },
//   },
//   {
//     plugin: require("/path/to/plugin2/gatsby-ssr.js"),
//     options: { ... },
//   },
// ]

const apis = require(`./api-ssr-docs`)

// Run the specified API in any plugins that have implemented it
module.exports = (api, args, defaultReturn, argTransform) => {
  if (!apis[api]) {
    console.log(`This API doesn't exist`, api)
  }

  // Run each plugin in series.
  // eslint-disable-next-line no-undef
  let results = plugins.map(plugin => {
    if (!plugin.plugin[api]) {
      return undefined
    }
    const result = plugin.plugin[api](args, plugin.options)
    if (result && argTransform) {
      args = argTransform({ args, result })
    }
    return result
  })

  // Filter out undefined results.
  results = results.filter(result => typeof result !== `undefined`)

  if (results.length > 0) {
    return results
  } else {
    return [defaultReturn]
  }
}
