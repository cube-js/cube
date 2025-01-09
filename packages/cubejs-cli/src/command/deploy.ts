import FormData from 'form-data';
import fs from 'fs-extra';
import path from 'path';
import cliProgress from 'cli-progress';
import { CommanderStatic } from 'commander';

import { DeployDirectory } from '../deploy';
import { logStage, displayError, event } from '../utils';
import { Config } from '../config';

interface Hashes {
  [key: string]: {
    hash: string;
  };
}

interface CloudReqResult {
  transaction: string;
  deploymentName: string;
}

const deploy = async ({ directory, auth, uploadEnv, token }: any) => {
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

  const config = new Config();
  const bar = new cliProgress.SingleBar({
    format: '- Uploading files | {bar} | {percentage}% || {value} / {total} | {file}',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true
  });

  const deployDir = new DeployDirectory({ directory });
  const fileHashes: any = await deployDir.fileHashes();

  const upstreamHashes: Hashes = await config.cloudReq({
    url: (deploymentId: string) => `build/deploy/${deploymentId}/files`,
    method: 'GET',
    auth
  });

  const { transaction, deploymentName }: CloudReqResult = await config.cloudReq({
    url: (deploymentId: string) => `build/deploy/${deploymentId}/start-upload`,
    method: 'POST',
    auth
  });

  if (uploadEnv) {
    const envVariables = await config.envFile(`${directory}/.env`);
    await config.cloudReq({
      url: (deploymentId) => `build/deploy/${deploymentId}/set-env`,
      method: 'POST',
      body: JSON.stringify({
        envVariables: JSON.stringify(envVariables),
      }),
      headers: {
        'Content-type': 'application/json'
      },
      auth
    });
  }

  await logStage(`Deploying ${deploymentName}...`, 'Cube Cloud CLI Deploy');

  const files = Object.keys(fileHashes);
  const fileHashesPosix = {};

  bar.start(files.length, 0, {
    file: ''
  });

  try {
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      bar.update(i, { file });

      const filePosix = file.split(path.sep).join(path.posix.sep);
      fileHashesPosix[filePosix] = fileHashes[file];

      if (!upstreamHashes[filePosix] || upstreamHashes[filePosix].hash !== fileHashes[file].hash) {
        const formData = new FormData();
        formData.append('transaction', JSON.stringify(transaction));
        formData.append('fileName', filePosix);
        formData.append('file', fs.createReadStream(path.join(directory, file)), {
          filename: path.basename(file),
          contentType: 'application/octet-stream'
        });

        await config.cloudReq({
          url: (deploymentId: string) => `build/deploy/${deploymentId}/upload-file`,
          method: 'POST',
          body: formData,
          auth,
          headers: formData.getHeaders()
        });
      }
    }
    bar.update(files.length, { file: 'Post processing...' });
    await config.cloudReq({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/finish-upload`,
      method: 'POST',
      body: JSON.stringify({
        transaction,
        files: fileHashesPosix
      }),
      headers: { 'Content-type': 'application/json' },
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
    .option('--upload-env', 'Upload .env file to CubeCloud')
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
