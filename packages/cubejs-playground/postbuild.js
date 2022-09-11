const fs = require('fs');
const path = require('path');

const distFolder = path.resolve(__dirname, 'lib');

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