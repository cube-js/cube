import { CommanderStatic } from 'commander';
import process from 'process';
import { Config } from '../../config';
import { createLogger, displayError } from '../../utils';
import { WatchService } from './WatchService';

type WatchOptions = {
  token?: string;
};

async function watch(options: WatchOptions) {
  const logger = createLogger();

  const config = new Config();
  if (options.token) {
    await config.addAuthToken(options.token);
  }

  logger.spin('Starting Dev Mode');

  const watchService = new WatchService(process.cwd(), {
    onDevModeStarted() {
      logger.ready('Dev Mode is ready');
      logger.persist();
    },
    onDidUpload() {
      logger.spin('Restarting Dev API');
    },
    onWillUpload() {
      logger.clear();
    },
    onBranchStatusChanged(data) {
      if (data.status === 'running') {
        logger.ready(`Dev API is ready at ${data.deploymentUrl} ${Date.now()}`);
      }
    },
  });
  await watchService.watch();
}

export function configureWatchCommand(program: CommanderStatic) {
  program
    .command('watch')
    .option('-p, --schema-path <schema-path>', 'Path to schema files. Default: schema')
    .option('--token <token>', 'Cube Cloud token')
    .action((options) => watch(options).catch((error) => displayError(error)))
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs watch');
    });
}
