module.exports = [{
      plugin: require('../node_modules/gatsby-remark-images/gatsby-browser.js'),
      options: {"plugins":[],"maxWidth":690},
    },{
      plugin: require('../node_modules/gatsby-remark-autolink-headers/gatsby-browser.js'),
      options: {"plugins":[]},
    },{
      plugin: require('../node_modules/gatsby-plugin-google-analytics/gatsby-browser.js'),
      options: {"plugins":[],"trackingId":"UA-70480064-3"},
    },{
      plugin: require('../node_modules/gatsby-plugin-snowplow-tracker/gatsby-browser.js'),
      options: {"plugins":[],"snippetHost":"//d1fc8wv8zag5ca.cloudfront.net","snippetVersion":"2.10.2","namespace":"scalacoll","collectorUri":"snowplow-collector.cube.dev","config":{"appId":"d3-dashboard-guide"}},
    },{
      plugin: require('../node_modules/gatsby-plugin-nprogress/gatsby-browser.js'),
      options: {"plugins":[],"color":"#c62828"},
    },{
      plugin: require('../node_modules/gatsby-plugin-catch-links/gatsby-browser.js'),
      options: {"plugins":[]},
    },{
      plugin: require('../node_modules/gatsby-plugin-twitter/gatsby-browser.js'),
      options: {"plugins":[]},
    },{
      plugin: require('../node_modules/gatsby-plugin-manifest/gatsby-browser.js'),
      options: {"plugins":[],"name":"Material UI Dashboard with Angular","short_name":"Material UI Dashboard with Angular","description":"How to build Angular Material Dashboard with Cube.js.","start_url":"","background_color":"#e0e0e0","theme_color":"#c62828","display":"minimal-ui","icon":"/Users/lonya/Desktop/cube.js/guides/guides-base/static/logos/icon.png","cache_busting_mode":"query","include_favicon":true,"legacy":true,"theme_color_in_head":true,"cacheDigest":"c79b6ee44cd5a2d74a48cb83170cce6f"},
    },{
      plugin: require('../node_modules/gatsby-plugin-offline/gatsby-browser.js'),
      options: {"plugins":[]},
    }]
