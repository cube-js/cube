import { CommanderStatic } from 'commander';
import chalk from 'chalk';
import semver from 'semver';
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
        // eslint-disable-next-line new-cap
        const Command = new OriginalCommandPackage.default([]);

        commandInfo
          .description(OriginalCommandPackage.default.description)
          .action(
            () => Command.run().catch(
              (e: any) => displayError(e.stack || e.message)
            )
          );

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
