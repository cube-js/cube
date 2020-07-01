const fs = require('fs');
const path = require('path');
const { Application } = require('typedoc');

const app = new Application({
  mode: 'Modules',
  module: 'CommonJS',
  experimentalDecorators: true,
  jsx: true,
  target: 'es2015',
});

const fixturesDir = './test/fixtures';
const inputFiles = app.expandInputFiles(['./test/stubs']);

if (!fs.existsSync(fixturesDir)) {
  fs.mkdirSync(fixturesDir);
}

inputFiles.forEach(file => {
  const result = app.convert(app.expandInputFiles([file]));
  fs.writeFileSync(`${fixturesDir}/${path.basename(file)}.json`, JSON.stringify(result, replacer));
  console.log(`[typedoc-plugin-markdown(task:fixtures)] writing ${path.basename(file)}.json fixture`);
});

function replacer(key, value) {
  if (
    key === 'parent' ||
    key === 'reflection' ||
    key === 'reflections' ||
    key === 'symbolMapping' ||
    key === 'file' ||
    key === 'cssClasses' ||
    key === '_alias' ||
    key === '_aliases' ||
    key === 'directory' ||
    key === 'packageInfo' ||
    key === 'files' ||
    key === 'readme'
  ) {
    return null;
  }
  return value;
}
