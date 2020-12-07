const path = require('path');
const CopyPlugin = require('copy-webpack-plugin');
const { addWebpackPlugin, override, addLessLoader } = require('customize-cra');

module.exports = override(
  addWebpackPlugin(
    new CopyPlugin({
      patterns: [{ from: path.resolve('node_modules', '@chartshq/muze/dist') }],
    })
  ),
  addLessLoader({
    lessOptions: {
      modifyVars: {
        '@primary-color': 'rgba(122, 119, 255, 1)',
        '@layout-header-background': '#43436b',
        '@layout-body-background': '#f3f3fc',
        '@menu-bg': 'none',
        '@btn-border-radius-base': '4px',
        '@btn-default-ghost-border': 'rgba(255, 255, 255, 0.35)',
        '@layout-header-height': '48px',
        '@layout-header-padding': '0 16px',
        '@font-size-base': '15px',
        '@font-family': 'DM Sans, sans-serif',
        '@divider-color': 'rgba(255, 255, 255, 30%)',
        '@text-color': 'darken(#fff, 15%)',
      },
      javascriptEnabled: true,
    },
  })
);
