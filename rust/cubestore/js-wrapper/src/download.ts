/* eslint-disable no-restricted-syntax */
import { downloadAndExtractFile, getHttpAgentForProxySettings } from '@cubejs-backend/shared';
import process from 'process';
import { Octokit } from '@octokit/core';
import * as path from 'path';

import { getTarget } from './utils';

export function getCubeStorePath() {
  return path.join(path.resolve(__dirname, '..', '..'), 'downloaded', 'latest');
}

export function getBinaryPath() {
  const binaryName = process.platform === 'win32' ? 'cubestored.exe' : 'cubestored';

  return path.join(getCubeStorePath(), 'bin', binaryName);
}

async function fetchRelease(version: string) {
  const client = new Octokit({
    auth: process.env.CUBEJS_GH_API_TOKEN,
    request: {
      agent: await getHttpAgentForProxySettings(),
    }
  });

  const { data } = await client.request('GET /repos/{owner}/{repo}/releases/tags/{tag}', {
    owner: 'cube-js',
    repo: 'cube.js',
    tag: `v${version}`
  });

  return data;
}

export async function downloadBinaryFromRelease() {
  // eslint-disable-next-line global-require
  const { version } = require('../../package.json');
  const cubestorePath = getCubeStorePath();
  const currentTarget = getTarget();

  const url = `https://github.com/cube-js/cube.js/releases/download/v${version}/cubestored-${currentTarget}.tar.gz`;

  try {
    await downloadAndExtractFile(url, {
      cwd: cubestorePath,
      showProgress: true,
    });
  } catch (e: any) {
    if (e.toString().includes('Not Found')) {
      const release = await fetchRelease(version);
      if (release) {
        if (release.assets.length === 0) {
          throw new Error(
            `There are no artifacts for Cube Store v${version}. Most probably it is still building. Please try again later.`
          );
        }

        throw new Error(
          `Cube Store v${version} Artifact for ${currentTarget} doesn't exist. Most probably it is still building. Please try again later.`
        );
      } else {
        throw new Error(
          `Unable to find Cube Store release v${version}. Most probably it was removed.`
        );
      }
    } else {
      throw e;
    }
  }
}
