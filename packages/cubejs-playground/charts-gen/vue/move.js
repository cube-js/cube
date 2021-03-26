const fs = require('fs-extra');
const path = require('path');
const { moveRecursively } = require('../utils');

const dirPath = path.resolve('../charts-dist/vue/vue-charts/dist');

let indexHtml = fs.readFileSync(path.join(dirPath, 'index.html'), 'utf-8');

indexHtml = indexHtml.replace(
  /<link rel="icon" href="\/favicon.ico"\s?\/>/,
  ''
);
indexHtml = indexHtml.replace(/\shref="/g, ' href="/chart-renderers/vue');
indexHtml = indexHtml.replace(/src="/g, 'src="/chart-renderers/vue');
indexHtml = indexHtml.replace('<head>', '<head><base href="/"/>');

fs.writeFileSync(path.join(dirPath, 'index.html'), indexHtml);

moveRecursively(dirPath, path.resolve('../public/chart-renderers/vue'), [
  /\.js$/,
  /\.css$/,
  /\.html$/,
]);
