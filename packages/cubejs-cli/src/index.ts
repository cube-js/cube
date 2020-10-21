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
      'You are running Node ' +
      currentNodeVersion +
      '.\n' +
      'Cube.js CLI requires Node 8 or higher. \n' +
      'Please update your version of Node.'
    )
  );
  process.exit(1);
}

require('./cli');
