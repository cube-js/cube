import program from 'commander';

import { configureDevServerCommand } from './command/dev-server';
import { configureServerCommand } from './command/server';
import { configureDeployCommand } from './command/deploy';
import { configureCreateCommand } from './command/create';
import { configureGenerateCommand } from './command/generate';
import { configureTokenCommand } from './command/token';
import { configureAuthCommand } from './command/auth';
import { loadCliManifest } from './utils';

const packageJson = loadCliManifest();

program.name(Object.keys(packageJson.bin)[0])
  .version(packageJson.version);

program
  .usage('<command> [options]')
  .on('--help', () => {
    console.log('');
    console.log('Use cubejs <command> --help for more information about a command.');
    console.log('');
  });

(async () => {
  await configureAuthCommand(program);
  await configureTokenCommand(program);
  await configureCreateCommand(program);
  await configureGenerateCommand(program);
  await configureDeployCommand(program);
  await configureDevServerCommand(program);
  await configureServerCommand(program);

  if (!process.argv.slice(2).length) {
    program.help();
  }

  program.parse(process.argv);
})();
