const path = require('path');
const TerserPlugin = require('terser-webpack-plugin')
const webpack = require('webpack');

module.exports = {
  context: __dirname,
  devtool: '#inline-source-map',
  entry: [
    './index.js',
  ],
  output: {
    path: __dirname + '/build',
    filename: 'bundle.js',
  },
  plugins: [
     new webpack.LoaderOptionsPlugin({
       debug: true
     }),
     new webpack.DefinePlugin({
      "process.env.CUBEJS_API_URL": `"${process.env.CUBEJS_API_URL || "https://statsbot.co/cubejs-api/v1"}"`
    }),
  ],
  module: {
    rules: [
      {
        test: /\.jsx?$/,
        loaders: ['babel-loader'],
        exclude: /node_modules/,
        include: [
          __dirname,
          path.join(__dirname, '..', 'src'),
        ],
      },
      {
        test: /\.css$/,
        loaders: ['style-loader', 'css-loader'],
      },
      {
        test: /\.svg$/,
        loader: 'file-loader'
      }
    ],
  },
  resolve: {
    extensions: ['.js', '.jsx']
  },
  node: {
    fs: 'empty'
  },
  optimization: {
    minimizer: [new TerserPlugin({
      parallel: true,
      terserOptions: {
        ecma: 6,
      },
      exclude: /@babel/
    })]
  }
};
