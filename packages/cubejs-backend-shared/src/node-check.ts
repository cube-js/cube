import process from 'process';
import color from '@oclif/color';

const leastSupportedVersion = 20;
const currentNodeVersion = process.versions.node;
const semver = currentNodeVersion.split('.');
const major = parseInt(<string> semver[0], 10);

if (major < leastSupportedVersion) {
  console.error(
    color.red(
      `You are running Node.js ${currentNodeVersion}.\n` +
      `Cube.js CLI requires Node.js ${leastSupportedVersion} or higher.\n` +
      'Please update your Node.js version.'
    )
  );
  process.exit(1);
}

if (major === (leastSupportedVersion + 1)) {
  process.emitWarning(
    color.red(
      `You are running Node.js ${currentNodeVersion}.\n` +
      `Support for Node.js ${leastSupportedVersion + 1} not guaranteed. Please upgrade to Node.js ${leastSupportedVersion + 2} or higher.`
    )
  );
}

if (major === leastSupportedVersion) {
  process.emitWarning(
    color.red(
      `You are running Node.js ${currentNodeVersion}.\n` +
      `Support for Node.js ${leastSupportedVersion} will be removed soon. Please upgrade to Node.js ${leastSupportedVersion + 2} or higher.`
    )
  );
}
