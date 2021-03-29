import fs from 'fs-extra';
import path from 'path';
import cliProgress from 'cli-progress';
import { CommanderStatic } from 'commander';
import { Config, CubeCloudClient, DeployController } from '@cubejs-backend/cloud';

import { logStage, displayError, event } from '../utils';

const deploy = async ({ directory, auth, token }: any) => {
  if (!(await fs.pathExists(path.join(process.cwd(), 'node_modules', '@cubejs-backend/server-core')))) {
    await displayError(
      '@cubejs-backend/server-core dependency not found. Please run deploy command from project root directory and ensure npm install has been run.'
    );
  }

  if (token) {
    const config = new Config();
    await config.addAuthToken(token);

    await event({
      event: 'Cube Cloud CLI Authenticate'
    });

    console.log('Token successfully added!');
  }

  const bar = new cliProgress.SingleBar({
    format: '- Uploading files | {bar} | {percentage}% || {value} / {total} | {file}',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true
  });

  const cubeCloudClient = new CubeCloudClient(auth);
  const deployController = new DeployController(cubeCloudClient, {
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
    .option('--upload-env', 'Upload .env file to CubeCloud')
    .option('--token <token>', 'Add auth token to CubeCloud')
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
}
