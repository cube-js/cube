const rp = require('request-promise');
const jwt = require('jsonwebtoken');
const fs = require('fs-extra');
const path = require('path');
const cliProgress = require('cli-progress');
const DeployDir = require('./DeployDir');
const { logStage } = require('./utils');

const cloudReq = (options) => {
  const { url, auth, ...restOptions } = options;
  const authorization = auth || process.env.CUBE_CLOUD_DEPLOY_AUTH;
  const payload = jwt.decode(authorization);
  if (!payload.url || !payload.deploymentId) {
    throw new (`Malformed token: ${authorization}`);
  }
  return rp({
    headers: {
      authorization
    },
    ...restOptions,
    url: `${payload.url}/${url(payload.deploymentId)}`,
    json: true
  });
};

exports.deploy = async ({ directory, auth }) => {
  const bar = new cliProgress.SingleBar({
    format: '- Uploading files | {bar} | {percentage}% || {value} / {total} | {file}',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true
  });

  const deployDir = new DeployDir({ directory });
  const fileHashes = await deployDir.fileHashes();
  const upstreamHashes = await cloudReq({
    url: (deploymentId) => `build/deploy/${deploymentId}/files`,
    method: 'GET',
    auth
  });
  const { transaction, deploymentName } = await cloudReq({
    url: (deploymentId) => `build/deploy/${deploymentId}/start-upload`,
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
        await cloudReq({
          url: (deploymentId) => `build/deploy/${deploymentId}/upload-file`,
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
    await cloudReq({
      url: (deploymentId) => `build/deploy/${deploymentId}/finish-upload`,
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
