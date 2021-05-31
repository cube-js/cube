import process from 'process';
import color from '@oclif/color';

const currentNodeVersion = process.versions.node;
const semver = currentNodeVersion.split('.');
const major = parseInt(<string> semver[0], 10);
const minor = parseInt(<string> semver[1], 10);

if (major < 10 || (major === 10 && minor < 8)) {
  console.error(
    color.red(
      `You are running Node.js ${currentNodeVersion}.\n` +
      'Cube.js CLI requires Node.js 10.8.0 or higher. \n' +
      'Please update your version of Node.js.'
    )
  );
  process.exit(1);
}

if (major === 10) {
  process.emitWarning(
    color.red(
      `You are running Node.js ${currentNodeVersion}.\n` +
      'Support for Node.js 10 will be removed soon. Please upgrade to Node.js 12 or higher.'
    )
  );
}
