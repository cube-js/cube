/*
eslint import/no-dynamic-require: 0
 */
/*
eslint global-require: 0
 */
import program from 'commander';
import { Create } from './commands/create.command';
import { Generate } from './commands/generate.command';

const Config = require('./Config');
const { deploy } = require('./deploy');
const { token, defaultExpiry, collect } = require('./token');
const { requireFromPackage, event, displayError } = require('./utils');

const packageJson = require('../package.json');

program.name(Object.keys(packageJson.bin)[0])
  .version(packageJson.version);

program
  .usage('<command> [options]')
  .on('--help', () => {
    console.log('');
    console.log('Use cubejs <command> --help for more information about a command.');
    console.log('');
  });

const commands = [
  new Create(),
  new Generate(),
];

for (const command of commands) {
  command.configure(program);
}

program
  .command('token')
  .option('-e, --expiry [expiry]', 'Token expiry. Set to 0 for no expiry', defaultExpiry)
  .option('-s, --secret [secret]', 'Cube.js app secret. Also can be set via environment variable CUBEJS_API_SECRET')
  .option('-p, --payload [values]', 'Payload. Example: -p foo=bar', collect, [])
  .description('Create JWT token')
  .action(
    (options) => token(options)
      .catch(e => displayError(e.stack || e))
  )
  .on('--help', () => {
    console.log('');
    console.log('Examples:');
    console.log('');
    console.log('  $ cubejs token -e "1 day" -p foo=bar -p cool=true');
  });

program
  .command('deploy')
  .description('Deploy project to Cube Cloud')
  .action(
    (options) => deploy({ directory: process.cwd(), ...options })
      .catch(e => displayError(e.stack || e))
  )
  .on('--help', () => {
    console.log('');
    console.log('Examples:');
    console.log('');
    console.log('  $ cubejs deploy');
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

if (!process.argv.slice(2).length) {
  program.help();
}

program.parse(process.argv);
