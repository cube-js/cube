import commander from 'commander';
import { ServerCommand } from './command/server';
import { DiagnosticCommand } from './command/diagnostic';
import { DevServerCommand } from './command/dev-server';

commander.name('cube')
  .version('0.0.1');

commander
  .usage('<command> [options]')
  .on('--help', () => {
    console.log('');
    console.log('Use cubejs <command> --help for more information about a command.');
    console.log('');
  });

const commands = [
  new ServerCommand(),
  new DevServerCommand(),
  new DiagnosticCommand(),
];

// eslint-disable-next-line no-restricted-syntax
for (const command of commands) {
  commander
    .command(command.getName())
    .description(command.getDescription())
    .action(() => command.execute())
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log(`  $ cube ${command.getName()}`);
    });
}

if (!process.argv.slice(2).length) {
  commander.help();
}

commander.parse(process.argv);
