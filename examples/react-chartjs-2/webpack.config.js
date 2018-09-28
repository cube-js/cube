var path = require('path');
var webpack = require('webpack');

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
  resolve: {
    alias: {
      'cubejs-client': path.join(__dirname, '../..', 'src/index.js'),
      '@cubejs-client/react': path.join(__dirname, '../..', 'packages/cubejs-react/src/index.js'),
    },
  },
  module: {
    rules: [
      {
        test: /\.js$/,
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
    ],
  },
};
