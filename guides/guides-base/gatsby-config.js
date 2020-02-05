const urljoin = require("url-join");
const path = require('path')

module.exports = (config, dirname) => {
  return {
    pathPrefix: config.pathPrefix === "" ? "/" : config.pathPrefix,
    siteMetadata: {
      siteUrl: urljoin(config.siteUrl, config.pathPrefix),
    },
    plugins: [
      "gatsby-plugin-react-helmet",
      "gatsby-plugin-lodash",
      {
        resolve: "gatsby-source-filesystem",
        options: {
          name: "assets",
          path: `${dirname}/static/`
        }
      },
      {
        resolve: "gatsby-source-filesystem",
        options: {
          name: "posts",
          path: `${dirname}/content/`
        }
      },
      {
        resolve: "gatsby-transformer-remark",
        options: {
          plugins: [
            {
              resolve: "gatsby-remark-images",
              options: {
                maxWidth: 690
              }
            },
            {
              resolve: "gatsby-remark-responsive-iframe"
            },
            {
              resolve: 'gatsby-remark-video',
              options: {
                width: "100%",
                height: 'auto',
                preload: 'auto',
                muted: true,
                autoplay: true,
                playsinline: true,
                controls: false,
                loop: true
              }
            },
            "gatsby-remark-copy-linked-files",
            "gatsby-remark-autolink-headers",
            "gatsby-remark-prismjs"
          ]
        }
      },
      {
        resolve: "gatsby-plugin-google-analytics",
        options: {
          trackingId: config.googleAnalyticsID
        }
      },
      {
        resolve: "gatsby-plugin-snowplow-tracker",
        options: {
            snippetHost: "//d1fc8wv8zag5ca.cloudfront.net",
            snippetVersion: "2.10.2",
            namespace: "scalacoll",
            collectorUri: "snowplow-collector.cube.dev",
            config: {
              appId: "d3-dashboard-guide"
            }
        }
      },
      {
        resolve: "gatsby-plugin-nprogress",
        options: {
          color: config.themeColor
        }
      },
      "gatsby-plugin-sharp",
      "gatsby-transformer-sharp",
      "gatsby-plugin-catch-links",
      "gatsby-plugin-twitter",
      "gatsby-plugin-sitemap",
      "gatsby-plugin-styled-components",
      {
        resolve: "gatsby-plugin-manifest",
        options: {
          name: config.siteTitle,
          short_name: config.siteTitleShort,
          description: config.siteDescription,
          start_url: config.pathPrefix,
          background_color: config.backgroundColor,
          theme_color: config.themeColor,
          display: "minimal-ui",
          icon: path.join(__dirname, './static/logos/icon.png')
        }
      },
      "gatsby-plugin-offline"
    ]
  };
}
