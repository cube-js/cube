import type { CommanderStatic } from 'commander';
import { displayError, requiredPackageExists, requireFromPackage } from '../utils';

async function serverCommand() {
  await requiredPackageExists('@cubejs-backend/server');

  const OriginalCommandPackage = await requireFromPackage('@cubejs-backend/server/dist/command/server');
  // eslint-disable-next-line new-cap
  const Command = new OriginalCommandPackage.default([]);
  return Command.run();
}

export function configureServerCommand(program: CommanderStatic) {
  program
    .command('server')
    .description('Run server in Production mode')
    .action(
      () => serverCommand()
        .catch((e) => displayError(e.stack || e))
    )
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs server');
    });
}
