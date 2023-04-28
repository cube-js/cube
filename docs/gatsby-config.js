require('dotenv')
  .config({
    path: `.env.${process.env.NODE_ENV}`,
  });

const PACKAGE_VERSION = require('../lerna.json').version;

const config = {
  siteMetadata: {
    title: 'Cube Docs',
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
    `gatsby-env-variables`,
    {
      resolve: 'gatsby-plugin-manifest',
      options: {
        icon: `src/favicon.png`,
      },
    },
    {
      resolve: `gatsby-plugin-layout`,
      options: {
        component: require.resolve(`./src/components/Layout/index.tsx`),
      },
    },
    {
      resolve: `gatsby-plugin-google-tagmanager`,
      options: {
        id: 'GTM-52W7VM2',
        routeChangeEventName: 'pageview',
      },
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
      resolve: 'gatsby-plugin-mdx',
      options: {
        extensions: [`.md`, `.mdx`],
        gatsbyRemarkPlugins: [
          {
            resolve: 'gatsby-remark-copy-linked-files',
            options: {
              destinationDir: 'content/',
            },
          },
          {
            resolve: 'gatsby-remark-images',
            options: {
              linkImagesToOriginal: false,
              maxWidth: 1150,
              wrapperStyle: 'margin-bottom: 24px',
            },
          },
          {
            resolve: `gatsby-remark-prismjs`,
            options: {
              inlineCodeMarker: null,
              noInlineHighlight: true,
              aliases: {
                dotenv: 'bash',
              },
              prompt: {
                user: 'user',
                host: 'localhost',
                global: false,
              },
              languageExtensions: [
                {
                  language: 'tree',
                  extend: 'json',
                  definition: {
                    'entry-line': [
                      {
                        pattern: /\|-- |├── /,
                        alias: 'line-h',
                      },
                      {
                        pattern: /\|   |│   /,
                        alias: 'line-v',
                      },
                      {
                        pattern: /`-- |└── /,
                        alias: 'line-v-last',
                      },
                      {
                        pattern: / {4}/,
                        alias: 'line-v-gap',
                      },
                    ],
                    'entry-dir': {
                      pattern: /.*[\/](?!\w).*/,
                      inside: {
                        // symlink
                        'operator': / -> /,
                      },
                    },
                    'entry-symlink': {
                      pattern: /.*\S.* (-> .*)/,
                      inside: {
                        'operator': / -> /,
                        'file': /(.*)/,
                      },
                    },
                    'entry-name': {
                      pattern: /.*\S.*/,
                      inside: {
                        // symlink
                        'operator': / -> /,
                      },
                    },
                  },
                },
              ],
            },
          },
          {
            resolve: 'gatsby-remark-find-replace',
            options: {
              replacements: {
                CURRENT_VERSION: PACKAGE_VERSION,
              },
              prefix: '%',
            },
          },
        ],
        remarkPlugins: [
          require('remark-math'),
          require('remark-html-katex'),
          require('./src/remark/plugins/link-environment-variables'),
        ],
      },
    },
    {
      resolve: 'gatsby-plugin-react-svg',
      options: {
        rule: {
          include: /\.inline\.svg$/,
        },
      },
    },
    'gatsby-plugin-netlify',
    // 'gatsby-plugin-percy',
  ],
};

module.exports = config;
