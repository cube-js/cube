console.log('Postbuild...', __dirname);

const fs = require('fs');
const path = require('path');

const distFolder = path.resolve(__dirname, 'lib');

console.log(`Postbuild... ${distFolder}`);

['cloud', 'rollup-designer'].forEach((name) => {
  fs.writeFileSync(
    path.join(distFolder, name, 'package.json'),
    JSON.stringify(
      {
        name: `@cubejs-client/playground/${name}`,
        type: 'module',
        module: 'index.js',
        types: `${name}/index.d.ts`,
      },
      null,
      2
    )
  );
});