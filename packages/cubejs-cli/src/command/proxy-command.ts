import { CommanderStatic } from 'commander';
import chalk from 'chalk';
import semver from 'semver';
import type { Command, flags } from '@oclif/command';
import { displayError, loadCliManifest, packageExists, requireFromPackage, requirePackageManifest } from '../utils';

export async function proxyCommand(program: CommanderStatic, command: string) {
  const serverPackageExists = packageExists('@cubejs-backend/server');

  const commandInfo = program
    .command(command);

  if (serverPackageExists) {
    const PackageManifiest = await requirePackageManifest('@cubejs-backend/server');

    if (PackageManifiest.cubejsCliVersion) {
      const cliManifiest = loadCliManifest();
      if (semver.satisfies(cliManifiest.version, PackageManifiest.cubejsCliVersion)) {
        const OriginalCommandPackage = await requireFromPackage(
          `@cubejs-backend/server/dist/command/${command}`
        );

        commandInfo
          .description(OriginalCommandPackage.default.description);

        if (OriginalCommandPackage.default.flags) {
          const commandFlags: Record<string, flags.IFlag<any>> = OriginalCommandPackage.default.flags;

          // eslint-disable-next-line no-restricted-syntax
          for (const [name, option] of Object.entries(commandFlags)) {
            commandInfo
              .option(`--${name}`, option.description || '', option.default);
          }
        }

        commandInfo.action(() => {
          try {
            // eslint-disable-next-line new-cap
            const CommandInstance: Command = new OriginalCommandPackage.default(process.argv.slice(3));
            CommandInstance.run();
          } catch (e) {
            displayError(e.stack || e.message);
          }
        });

        return;
      }

      const message = `${chalk.red('Unavailable.')} @cubejs-backend/server inside current directory requires ` +
        `cubejs-cli (${PackageManifiest.cubejsCliVersion}).`;

      commandInfo
        .description(
          message
        )
        .action(
          () => displayError(message)
        );

      return;
    }

    const message = `${chalk.red('Unavailable.')} Please upgrade @cubejs-backend/server.`;

    commandInfo
      .description(
        message
      )
      .action(
        () => displayError(message)
      );
  } else {
    const message = `${chalk.red('Unavailable.')} Please run this command from project directory.`;

    commandInfo
      .description(
        message
      )
      .action(
        () => displayError(message)
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
