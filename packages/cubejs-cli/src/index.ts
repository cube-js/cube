#!/usr/bin/env node
/*
eslint no-var: 0
*/
/*
eslint prefer-template: 0
 */
import chalk from 'chalk';

const currentNodeVersion = process.versions.node;
const semver = currentNodeVersion.split('.');
const major = parseInt(<string> semver[0], 10);

if (major < 8) {
  console.error(
    chalk.red(
      'You are running Node.js ' + currentNodeVersion + '.\n' +
      'Cube.js CLI requires Node.js 8 or higher. \n' +
      'Please update your version of Node.js.'
    )
  );
  process.exit(1);
}

if (major < 10) {
  process.emitWarning(
    chalk.red(
      'You are running Node.js ' + currentNodeVersion + '.\n' +
      'Support for Node.js 8 will be removed soon. Please upgrade to Node.js 10 or higher.'
    )
  );
}

require('./cli');
