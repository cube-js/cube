import type { CommanderStatic } from 'commander';
import { displayError, requireFromPackage, requiredPackageExists } from '../utils';

async function devServerCommand() {
  await requiredPackageExists('@cubejs-backend/server');

  const OriginalCommandPackage = await requireFromPackage('@cubejs-backend/server/dist/command/dev-server');
  // eslint-disable-next-line new-cap
  const Command = new OriginalCommandPackage.default([]);
  return Command.run();
}

export function configureDevServerCommand(program: CommanderStatic) {
  program
    .command('dev-server')
    .description('Run server in Development mode')
    .action(
      () => devServerCommand()
        .catch((e) => displayError(e.stack || e))
    )
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs dev-server');
    });
}
