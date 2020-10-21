import fs from 'fs-extra';
import path from 'path';
import cliProgress from 'cli-progress';
import { CommanderStatic } from 'commander';
import { DeployDirectory } from '../deploy';
import { logStage, displayError } from '../utils';
import { Config } from '../config';

const deploy = async ({ directory, auth }: any) => {
  if (!(await fs.pathExists(path.join(process.cwd(), 'node_modules', '@cubejs-backend/server-core')))) {
    await displayError(
      '@cubejs-backend/server-core dependency not found. Please run deploy command from project root directory and ensure npm install has been run.'
    );
  }

  const config = new Config();
  await config.loadDeployAuth();
  const bar = new cliProgress.SingleBar({
    format: '- Uploading files | {bar} | {percentage}% || {value} / {total} | {file}',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true
  });

  const deployDir = new DeployDirectory({ directory });
  const fileHashes: any = await deployDir.fileHashes();
  const upstreamHashes = await config.cloudReq({
    url: (deploymentId: string) => `build/deploy/${deploymentId}/files`,
    method: 'GET',
    auth
  });

  const { transaction, deploymentName } = await config.cloudReq({
    url: (deploymentId: string) => `build/deploy/${deploymentId}/start-upload`,
    method: 'POST',
    auth
  });

  await logStage(`Deploying ${deploymentName}...`, 'Cube Cloud CLI Deploy');

  const files = Object.keys(fileHashes);
  bar.start(files.length, 0, {
    file: ''
  });

  try {
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      bar.update(i, { file });

      if (!upstreamHashes[file] || upstreamHashes[file].hash !== fileHashes[file].hash) {
        await config.cloudReq({
          url: (deploymentId: string) => `build/deploy/${deploymentId}/upload-file`,
          method: 'POST',
          formData: {
            transaction: JSON.stringify(transaction),
            fileName: file,
            file: {
              value: fs.createReadStream(path.join(directory, file)),
              options: {
                filename: path.basename(file),
                contentType: 'application/octet-stream'
              }
            }
          },
          auth
        });
      }
    }
    bar.update(files.length, { file: 'Post processing...' });
    await config.cloudReq({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/finish-upload`,
      method: 'POST',
      body: {
        transaction,
        files: fileHashes
      },
      auth
    });
  } finally {
    bar.stop();
  }

  await logStage('Done 🎉', 'Cube Cloud CLI Deploy Success');
};

export function configureDeployCommand(program: CommanderStatic) {
  program
    .command('deploy')
    .description('Deploy project to Cube Cloud')
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
