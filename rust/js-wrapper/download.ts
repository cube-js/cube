/* eslint-disable no-restricted-syntax */
import tar from 'tar';
import fs, { mkdirSync, WriteStream } from 'fs';
import fetch, { Headers, Request, Response } from 'node-fetch';
import { throttle } from 'throttle-debounce';
import { internalExceptions } from '@cubejs-backend/shared';
import bytes from 'bytes';
import cli from 'cli-ux';
import process from 'process';
import { Octokit } from '@octokit/core';
import * as path from 'path';

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
      response.body.on('end', () => {
        resolve();
      });
    }
  );
}

export async function downloadAndExtractFile(url: string, fileName: string, workingDirectory: string) {
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

  try {
    mkdirSync(path.join(workingDirectory, 'downloaded'));
    mkdirSync(path.join(workingDirectory, 'downloaded', 'latest'));
  } catch (e) {
    internalExceptions(e);
  }

  const writer = tar.x({
    cwd: path.join(workingDirectory, 'downloaded', 'latest'),
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

async function fetchRelease() {
  // eslint-disable-next-line global-require
  const { version } = require('../package.json');

  const client = new Octokit();

  const { data } = await client.request('GET /repos/{owner}/{repo}/releases/tags/{tag}', {
    owner: 'cube-js',
    repo: 'cube.js',
    tag: `v${version}`
  });

  return data;
}

export async function downloadBinaryFromRelease() {
  const release = await fetchRelease();
  if (release) {
    if (release.assets.length === 0) {
      throw new Error('No assets in release');
    }

    const target = getTarget();

    for (const asset of release.assets) {
      const fileName = asset.name.substr(0, asset.name.length - 7);
      if (fileName.startsWith('cubestored-')) {
        const assetTarget = fileName.substr('cubestored-'.length);
        if (assetTarget === target) {
          await downloadAndExtractFile(asset.browser_download_url, asset.name, path.resolve(__dirname, '..'));
        }
      }
    }
  }
}
