/* eslint-disable no-restricted-syntax */
import * as fs from 'fs';
import { Octokit } from '@octokit/core';
import * as process from 'process';
import fetch, { Request, Headers, Response } from 'node-fetch';
import cli from 'cli-ux';
import tar from 'tar';
import { throttle } from 'throttle-debounce';
import bytes from 'bytes'
import { WriteStream } from 'fs';

const { version } = require('../package.json');

if (process.env.CUBESTORE_SKIP_POST_INSTALL) {
  process.exit(0);
}

const client = new Octokit();

async function fetchRelease() {
  const { data } = await client.request('GET /repos/{owner}/{repo}/releases/tags/{tag}', {
    owner: 'cube-js',
    repo: 'cube.js',
    tag: `v${version}`
  });

  return data;
}

async function extractFile(fileName: string) {
  await new Promise<void>(
    (resolve, reject) => {
      // @ts-ignore
      tar.extract({ file: fileName }, (err) => {
        if (err) {
          return reject(err);
        }

        resolve();
      });
    }
  );

  try {
    fs.unlinkSync(fileName);
  } catch (err) {
    // console.error(err);
  }
}

type ByteProgressCallback = (info: { progress: number, eta: number, speed }) => void

async function streamWithProgress(
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

  return new Promise((resolve, reject) => {
    response.body.on('data', (chunk) => {
      done += chunk.length;
      return throttled();
    });

    response.body.on('end', () => {
      resolve();
    });
  });
}

async function downloadFile(url: string, fileName: string) {
  const request = new Request(url, {
    headers: new Headers({
      'Content-Type': 'application/octet-stream'
    })
  });

  const response = await fetch(request);
  if (!response.ok) {
    throw new Error(`unexpected response ${response.statusText}`);
  }

  try {
    fs.unlinkSync(fileName);
  } catch (err) {
    // console.error(err);
  }

  const bar = cli.progress({
    format: 'Downloading from GitHub [{bar}] {percentage}% | Speed: {speed}',
  });
  bar.start(100, 0);

  const writer = await new Promise<WriteStream>((resolve, reject) => {
    const stream = fs.createWriteStream(fileName);

    stream.on('open', () => {
      resolve(stream);
    });

    stream.on('error', (e) => {
      reject(e);
    });
  });

  await streamWithProgress(response, writer, ({ progress, speed, eta }) => {
    bar.update(progress, {
      speed,
      eta
    });
  });

  bar.stop();
}

function getTarget(): string {
  if (process.arch !== 'x64') {
    throw new Error(
      `You are using ${process.arch} architecture which is not supported by Cube Store`,
    );
  }

  switch (process.platform) {
    case 'linux':
      return 'x86_64-unknown-linux-gnu';
    case 'darwin':
      return 'x86_64-apple-darwin';
    default:
      throw new Error(
        `You are using ${process.env} which is not supported by Cube Store`,
      );
  }
}

(async () => {
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
          await downloadFile(asset.browser_download_url, asset.name);
          await extractFile(asset.name);
        }
      }
    }
  }
})();
