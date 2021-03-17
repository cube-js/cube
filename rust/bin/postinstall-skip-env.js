const fs = require('fs');

if (process.env.CUBESTORE_SKIP_POST_INSTALL) {
  console.log('Skipping Cube Store Post Installing..');
  return;
}

if (!fs.existsSync('../dist/post-install') && fs.existsSync('../tsconfig.json')) {
  console.log('Skipping post-install because it was not compiled');
  return;
}

require('../dist/post-install');
