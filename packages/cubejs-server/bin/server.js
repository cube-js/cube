#!/usr/bin/env node

require('@oclif/command').run()
  .then(require('@oclif/command/flush'))
  .catch(require('@oclif/errors/handle'));

