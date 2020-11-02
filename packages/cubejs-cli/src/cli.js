/*
eslint import/no-dynamic-require: 0
 */
/*
eslint global-require: 0
 */

import program from 'commander';

import { configureDevServerCommand } from './command/dev-server';
import { configureServerCommand } from './command/server';
import { configureDeployCommand } from './command/deploy';
import {
  event,
  displayError,
  loadCliManifest,
} from './utils';
import { Config } from './config';
import { configureCreateCommand } from './command/create';
import { configureGenerateCommand } from './command/generate';
import { configureTokenCommand } from './command/token';

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

const authenticate = async (currentToken) => {
  const config = new Config();
  await config.addAuthToken(currentToken);
  await event('Cube Cloud CLI Authenticate');
  console.log('Token successfully added!');
};

program
  .command('auth <token>')
  .description('Authenticate access to Cube Cloud')
  .action(
    (currentToken) => authenticate(currentToken)
      .catch(e => displayError(e.stack || e))
  )
  .on('--help', () => {
    console.log('');
    console.log('Examples:');
    console.log('');
    console.log('  $ cubejs auth eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkZXBsb3ltZW50SWQiOiIxIiwidXJsIjoiaHR0cHM6Ly9leGFtcGxlcy5jdWJlY2xvdWQuZGV2IiwiaWF0IjoxNTE2MjM5MDIyfQ.La3MiuqfGigfzADl1wpxZ7jlb6dY60caezgqIOoHt-c');
    console.log('  $ cubejs deploy');
  });

(async () => {
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
