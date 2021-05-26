import { CommanderStatic } from 'commander';
import chalk from 'chalk';
import semver from 'semver';
import {
  isDockerImage,
  packageExists,
  requireFromPackage,
  requirePackageManifest,
} from '@cubejs-backend/shared';
import type { Command, flags } from '@oclif/command';
import { displayError, loadCliManifest, } from '../utils';

export async function proxyCommand(program: CommanderStatic, command: string) {
  const relative = isDockerImage();
  const serverPackageExists = packageExists('@cubejs-backend/server', relative);

  const commandInfo = program
    .command(command);

  if (serverPackageExists) {
    const packageManifest = await requirePackageManifest<any>('@cubejs-backend/server', {
      relative,
    });
    if (packageManifest.cubejsCliVersion) {
      const cliManifest = loadCliManifest();
      if (semver.satisfies(cliManifest.version, packageManifest.cubejsCliVersion)) {
        const OriginalCommandPackage = requireFromPackage<any>(
          `@cubejs-backend/server/dist/command/${command}.js`,
          {
            relative,
          }
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

        commandInfo.action(async () => {
          try {
            // eslint-disable-next-line new-cap
            const CommandInstance: Command = new OriginalCommandPackage.default(process.argv.slice(3));
            await CommandInstance.run();
          } catch (e) {
            displayError(e.stack || e.message);
          }
        });

        return;
      }

      const message = `${chalk.red('Unavailable.')} @cubejs-backend/server inside current directory requires ` +
        `cubejs-cli (${packageManifest.cubejsCliVersion}).`;

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
