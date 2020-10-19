import { CommanderStatic } from 'commander';
import { displayError, packageExists, requireFromPackage } from '../utils';
import chalk from 'chalk';

export async function proxyCommand(program: CommanderStatic, command: string) {
  const serverPackageExists = packageExists('@cubejs-backend/server');

  const commandInfo = program
    .command(command);

  if (serverPackageExists) {
    const OriginalCommandPackage = await requireFromPackage(`@cubejs-backend/server/dist/command/${command}`);
    // eslint-disable-next-line new-cap
    const Command = new OriginalCommandPackage.default([]);

    commandInfo
      .description(OriginalCommandPackage.default.description)
      .action(
        () => Command.run().catch(
          (e: any) => displayError(e.stack || e.message)
        )
      );
  } else {
    commandInfo
      .description(
        chalk.red('Unavailable.') + ' Please run this command from project directory.'
      )
      .action(
        () => displayError('Unavailable. Please run this command from project directory.')
      );
  }

  commandInfo
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log(`  $ cubejs ${command}`);
    });
}
