if (process.env.CUBESTORE_SKIP_POST_INSTALL) {
  console.log('Skipping Cube Store Post Installing..');
  process.exit(0);
}

require('../dist/post-install');
