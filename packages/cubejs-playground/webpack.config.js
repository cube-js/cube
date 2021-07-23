const path = require('path');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const webpack = require('webpack');

const { LESS_VARIABLES } = require('./src/variables');

module.exports = {
  entry: './src/index.tsx',
  plugins: [
    new MiniCssExtractPlugin({
      filename: 'antd.min.css',
    }),
    new webpack.LoaderOptionsPlugin({
      options: {
        external: [
          'react',
          'react-dom',
          'react/jsx-runtime',
          'react-router',
          'prop-types',
          'styled-components',
        ],
      },
    }),
  ],
  module: {
    rules: [
      {
        test: /\.less$/i,

        use: [
          {
            loader: MiniCssExtractPlugin.loader,
            // options: {
            //   filename: 'antd.min.css'
            // }
          },
          {
            loader: 'css-loader',
          },
          {
            loader: 'less-loader',
            options: {
              lessOptions: {
                javascriptEnabled: true,
                modifyVars: LESS_VARIABLES,
              },
            },
          },
        ],
      },
      {
        test: /\.svg$/,
        use: [
          {
            loader: 'svg-url-loader',
            options: {
              limit: 10000,
            },
          },
        ],
      },
      {
        test: /\.tsx?$/,
        exclude: /node_modules/,
        use: 'ts-loader'
      },
    ],
  },
  resolve: {
    extensions: ['.tsx', '.ts', '.js'],
  },
  output: {
    filename: 'cubejs-playground.esm.js',
    path: path.resolve(__dirname, 'lib'),
    libraryTarget: 'module'
  },
  experiments: {
    outputModule: true
  },
  mode: 'production'
};
