const fs = require('fs');
const path = require('path');

if (process.env.CUBESTORE_SKIP_POST_INSTALL) {
  console.log('Skipping Cube Store Post Installing..');
  return;
}

if (!fs.existsSync(path.join(__dirname, '..', 'dist', 'post-install.js')) && fs.existsSync(path.join(__dirname, '..', 'tsconfig.json'))) {
  console.log('Skipping post-install because it was not compiled');
  return;
}

require('../dist/post-install');
