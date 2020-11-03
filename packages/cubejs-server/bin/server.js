#!/usr/bin/env node

require('source-map-support/register');

require('@oclif/command').run()
  .then(require('@oclif/command/flush'))
  .catch(require('@oclif/errors/handle'));
