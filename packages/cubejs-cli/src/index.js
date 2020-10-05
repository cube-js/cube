#!/usr/bin/env node
/*
eslint no-var: 0
*/
/*
eslint prefer-template: 0
 */
var chalk = require('chalk');

var currentNodeVersion = process.versions.node;
var semver = currentNodeVersion.split('.');
var major = semver[0];

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

require('./cubejsCli');
