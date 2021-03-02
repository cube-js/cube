/* eslint-disable no-restricted-syntax */
import tar from 'tar';
import fs, { WriteStream } from 'fs';
import fetch, { Headers, Request, Response } from 'node-fetch';
import { throttle } from 'throttle-debounce';
import { internalExceptions } from '@cubejs-backend/shared';
import bytes from 'bytes';
import cli from 'cli-ux';
import process from 'process';
import { Octokit } from '@octokit/core';
import * as path from 'path';
import { mkdirpSync } from 'fs-extra';

import { detectLibc } from './utils';

type ByteProgressCallback = (info: { progress: number, eta: number, speed: string }) => void;

export async function streamWithProgress(
  response: Response,
  writer: fs.WriteStream,
  progressCallback: ByteProgressCallback
): Promise<void> {
  const total = parseInt(response.headers.get('Content-Length') || '0', 10);
  const startedAt = Date.now();

  let done = 0;

  const throttled = throttle(
    10,
    () => {
      const elapsed = (Date.now() - startedAt) / 1000;
      const rate = done / elapsed;
      const speed = `${bytes(rate)}/s`;
      const estimated = total / rate;
      const progress = parseInt(<any>((done / total) * 100), 10);
      const eta = estimated - elapsed;

      progressCallback({
        progress,
        eta,
        speed
      });
    },
  );

  response.body.pipe(writer);
  response.body.on('data', (chunk) => {
    done += chunk.length;
    throttled();
  });

  return new Promise(
    (resolve) => {
      // Wait before writer will finish, because response can be done earlier then extracting
      writer.on('finish', () => {
        resolve();
      });
    }
  );
}

export function getBinaryPath() {
  const binaryName = process.platform === 'win32' ? 'cubestored.exe' : 'cubestored';

  return path.join(path.resolve(__dirname, '..'), 'downloaded', 'latest', 'bin', binaryName);
}

export async function downloadAndExtractFile(url: string) {
  const request = new Request(url, {
    headers: new Headers({
      'Content-Type': 'application/octet-stream'
    })
  });

  const response = await fetch(request);
  if (!response.ok) {
    throw new Error(`unexpected response ${response.statusText}`);
  }

  const bar = cli.progress({
    format: 'Downloading from GitHub [{bar}] {percentage}% | Speed: {speed}',
  });
  bar.start(100, 0);

  const cubestorePath = path.dirname(getBinaryPath());

  try {
    mkdirpSync(cubestorePath);
  } catch (e) {
    internalExceptions(e);
  }

  const writer = tar.x({
    cwd: cubestorePath,
  });

  await streamWithProgress(response, <WriteStream>writer, ({ progress, speed, eta }) => {
    bar.update(progress, {
      speed,
      eta
    });
  });

  bar.stop();
}

export function getTarget(): string {
  if (process.arch !== 'x64') {
    throw new Error(
      `You are using ${process.arch} architecture which is not supported by Cube Store`,
    );
  }

  switch (process.platform) {
    case 'win32':
      return 'x86_64-pc-windows-gnu';
    case 'linux':
      return `x86_64-unknown-linux-${detectLibc()}`;
    case 'darwin':
      return 'x86_64-apple-darwin';
    default:
      throw new Error(
        `You are using ${process.env} which is not supported by Cube Store`,
      );
  }
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
        return downloadAndExtractFile(asset.browser_download_url);
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
