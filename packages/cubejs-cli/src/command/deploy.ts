import fs from 'fs-extra';
import path from 'path';
import cliProgress from 'cli-progress';
import { CommanderStatic } from 'commander';
import { AuthObject, CubeCloudClient, DeployController } from '@cubejs-backend/cloud';
import { ConfigCli } from '../config';

import { logStage, displayError, event } from '../utils';

type DeployOptions = {
  directory: string,
  auth: AuthObject,
  uploadEnv: boolean,
  replaceEnv: boolean,
  token: string
};

const deploy = async ({ directory, auth, uploadEnv, replaceEnv, token }: DeployOptions) => {
  if (!(await fs.pathExists(path.join(process.cwd(), 'node_modules', '@cubejs-backend/server-core')))) {
    await displayError(
      '@cubejs-backend/server-core dependency not found. Please run deploy command from project root directory and ensure npm install has been run.'
    );
  }

  const config = new ConfigCli();
  if (token) {
    await config.addAuthToken(token);
    await event({ event: 'Cube Cloud CLI Authenticate' });
    console.log('Token successfully added!');
  }

  const bar = new cliProgress.SingleBar({
    format: '- Uploading files | {bar} | {percentage}% || {value} / {total} | {file}',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true
  });

  const envVariables = uploadEnv || replaceEnv ? await config.envFile(`${directory}/.env`) : {};

  const cubeCloudClient = new CubeCloudClient(auth || (await config.deployAuthForCurrentDir()));
  const deployController = new DeployController(cubeCloudClient, { envVariables, replaceEnv }, {
    onStart: async (deploymentName, files) => {
      await logStage(`Deploying ${deploymentName}...`, 'Cube Cloud CLI Deploy');
      bar.start(files.length, 0, {
        file: ''
      });
    },
    onUpdate: (i, { file }) => {
      bar.update(i, { file });
    },
    onUpload: (files) => {
      bar.update(files.length, { file: 'Post processing...' });
    },
    onFinally: () => {
      bar.stop();
    }
  });

  await deployController.deploy(directory);
  await logStage('Done 🎉', 'Cube Cloud CLI Deploy Success');
};

export function configureDeployCommand(program: CommanderStatic) {
  program
    .command('deploy')
    .description('Deploy project to Cube Cloud')
    .option('--upload-env', 'Use .env file to populate environment variables in Cube Cloud. Only set them once during the very first upload for this deployment')
    .option('--replace-env', 'Use .env file to populate environment variables in Cube Cloud. Replace them with new ones during every upload for this deployment')
    .option('--token <token>', 'Add auth token to CubeCloud')
    .option('--directory [path]', 'Specify path to conf directory', './')
    .action(
      (options) => deploy({
        ...options,
        directory: path.join(process.cwd(), options.directory)
      })
        .catch(e => displayError(e.stack || e))
    )
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs deploy');
    });
}
