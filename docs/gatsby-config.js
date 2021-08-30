require('dotenv').config({
  path: `.env.${process.env.NODE_ENV}`,
});

const PACKAGE_VERSION = require('../lerna.json').version;

const config = {
  siteMetadata: {
    title: 'Cube.js Docs',
    siteUrl: `https://cube.dev`,
  },
  pathPrefix: process.env.PATH_PREFIX,
  plugins: [
    'gatsby-plugin-react-helmet',
    'gatsby-plugin-sass',
    'gatsby-plugin-antd',
    'gatsby-plugin-catch-links',
    'gatsby-plugin-sharp',
    'gatsby-plugin-root-import',
    `gatsby-plugin-sitemap`,
    {
      resolve: 'gatsby-plugin-manifest',
      options: {
        icon: `src/favicon.png`,
      }
    },
    {
      resolve: `gatsby-plugin-layout`,
      options: {
        component: require.resolve(`./src/components/Layout/index.tsx`)
      }
    },
    {
      resolve: `gatsby-plugin-google-analytics`,
      options: {
        trackingId: "UA-70480064-3"
      }
    },
    {
      resolve: 'gatsby-source-filesystem',
      options: {
        name: 'cubejs-docs',
        path: `${__dirname}/content/`,
      },
    },
    {
      resolve: 'gatsby-plugin-typescript',
      options: {
        isTSX: true, // defaults to false
        jsxPragma: 'jsx', // defaults to "React"
        allExtensions: true, // defaults to false
      },
    },
    {
      resolve: 'gatsby-transformer-remark',
      options: {
        plugins: [
          {
            resolve: 'gatsby-remark-copy-linked-files',
            options: {
              destinationDir: 'content/',
            }
          },
          {
            resolve: 'gatsby-remark-images',
            options: {
              linkImagesToOriginal: false,
              maxWidth: 1150,
              wrapperStyle: 'margin-bottom: 24px'
            }
          },
          `gatsby-remark-mathjax-ssr`,
          {
            resolve: 'gatsby-remark-find-replace',
            options: {
              replacements: {
                CURRENT_VERSION: PACKAGE_VERSION,
              },
              prefix: '%',
            },
          },
          {
            resolve: 'gatsby-remark-custom-blocks',
            options: {
              blocks: {
                danger: {
                  classes: 'danger',
                  title: 'optional',
                },
                info: {
                  classes: 'info',
                  title: 'optional',
                },
                warning: {
                  classes: 'warning',
                  title: 'optional',
                },
                success: {
                  classes: 'success',
                  title: 'optional',
                },
              },
            },
          },
        ]
      }
    },
    'gatsby-redirect-from',
    'gatsby-plugin-meta-redirect',
    'gatsby-plugin-percy',
  ],
};

module.exports = config;
