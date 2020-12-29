const fs = require('fs-extra');
const path = require('path');
const { moveRecursively } = require('../utils');

const dirPath = path.resolve('../charts-dist/angular/angular-charts/dist/angular-charts');

let indexHtml = fs.readFileSync(path.join(dirPath, 'index.html'), 'utf-8');
indexHtml = indexHtml.replace(
  /" href="/g,
  '" href="/chart-renderers/angular/'
);
indexHtml = indexHtml.replace(
  /src="/g,
  'src="/chart-renderers/angular/'
);

console.log(indexHtml)

fs.writeFileSync(path.join(dirPath, 'index.html'), indexHtml);

moveRecursively(dirPath, path.resolve('../public/chart-renderers/angular'), [
  /\.js$/,
  /\.css$/,
  /\.html$/,
]);
