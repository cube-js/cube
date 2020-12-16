const fs = require('fs-extra');
const path = require('path');
const { moveRecursively } = require('../utils');

const dirPath = path.resolve('../charts-dist/react/react-charts/build');

let indexHtml = fs.readFileSync(path.join(dirPath, 'index.html'), 'utf-8');
indexHtml = indexHtml.replace('<head>', '<head><base href="/"/>');
indexHtml = indexHtml.replace(
  /"\/static\//g,
  '"/chart-renderers/react/static/'
);

fs.writeFileSync(path.join(dirPath, 'index.html'), indexHtml);

moveRecursively(dirPath, path.resolve('../public/chart-renderers/react'), [
  /\.js$/,
  /\.css$/,
  /\.html$/,
]);
