#!/usr/bin/env node

require('source-map-support/register');

const argv = process.argv.slice(2);

/**
 * We are going to replace dev-server command by CUBEJS_DEV_MODE
 * It's why we are planing to make cubejs-server single command CLI
 */
if (argv.length === 0) {
  argv.push('server');
}

require('@oclif/command').run(argv)
  .then(require('@oclif/command/flush'))
  .catch(require('@oclif/errors/handle'));
