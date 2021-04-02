/* eslint-disable no-restricted-syntax */
import { downloadAndExtractFile } from '@cubejs-backend/shared';
import process from 'process';
import { Octokit } from '@octokit/core';
import * as path from 'path';

import { getTarget } from './utils';

export function getCubeStorePath() {
  return path.join(path.resolve(__dirname, '..'), 'downloaded', 'latest');
}

export function getBinaryPath() {
  const binaryName = process.platform === 'win32' ? 'cubestored.exe' : 'cubestored';

  return path.join(getCubeStorePath(), 'bin', binaryName);
}

async function fetchRelease(version: string) {
  const client = new Octokit();

  const { data } = await client.request('GET /repos/{owner}/{repo}/releases/tags/{tag}', {
    owner: 'cube-js',
    repo: 'cube.js',
    tag: `v${version}`
  });

  return data;
}

function parseInfoFromAssetName(assetName: string): { target: string, type: string, format: string } | null {
  if (assetName.startsWith('cubestored-')) {
    const fileName = assetName.slice('cubestored-'.length);
    const targetAndType = fileName.slice(0, fileName.indexOf('.'));
    const format = fileName.slice(fileName.indexOf('.') + 1);

    if (targetAndType.endsWith('-shared')) {
      return {
        target: targetAndType.substr(0, targetAndType.length - '-shared'.length),
        format,
        type: 'shared'
      };
    }

    return {
      target: targetAndType,
      format,
      type: 'static'
    };
  }

  return null;
}

export async function downloadBinaryFromRelease() {
  // eslint-disable-next-line global-require
  const { version } = require('../package.json');

  const release = await fetchRelease(version);
  if (release) {
    if (release.assets.length === 0) {
      throw new Error(
        `There are no artifacts for Cube Store v${version}. Most probably it is still building. Please try again later.`
      );
    }

    const currentTarget = getTarget();

    for (const asset of release.assets) {
      const assetInfo = parseInfoFromAssetName(asset.name);
      if (assetInfo && assetInfo.target === currentTarget
        && assetInfo.type === 'static' && assetInfo.format === 'tar.gz'
      ) {
        const cubestorePath = getCubeStorePath();

        return downloadAndExtractFile(asset.browser_download_url, {
          cwd: cubestorePath,
          showProgress: true,
        });
      }
    }

    throw new Error(
      `Cube Store v${version} Artifact for ${process.platform} is not found. Most probably it is still building. Please try again later.`
    );
  }

  throw new Error(
    `Unable to find Cube Store release v${version}. Most probably it was removed.`
  );
}
