/* eslint-disable import/no-extraneous-dependencies */
const recursive = require('recursive-readdir');
const fs = require('fs-extra');
const path = require('path');

const generateScaffolding = async () => {
  const scaffoldingPath = 'src/scaffolding';
  const fileNames = await recursive(scaffoldingPath);
  const files = (await Promise.all(
    fileNames.map(async fileName => ({
      [path.relative(scaffoldingPath, fileName)]: await fs.readFile(fileName, 'utf8')
    }))
  )).reduce((a, b) => ({ ...a, ...b }), {});

  const scaffoldingSources = `export default ${JSON.stringify(files)};`;

  const fileName = 'src/codegen/ScaffoldingSources.js';
  await fs.outputFile(fileName, scaffoldingSources);
  console.log(`Done generating ${fileName}`);
};

generateScaffolding().catch(e => console.error(e));
