import tar from 'tar';
import fs, { WriteStream } from 'fs';
import fetch, { Headers, Request, Response } from 'node-fetch';
import { throttle } from 'throttle-debounce';
import bytes from 'bytes';
import cli from 'cli-ux';
import process from 'process';
import { Octokit } from '@octokit/core';
import * as path from 'path';

export async function extractFile(fileName: string, workingDirectory: string) {
  await new Promise<void>(
    (resolve, reject) => {
      // @ts-ignore
      tar.extract({ file: path.join(workingDirectory, fileName), cwd: workingDirectory }, (err) => {
        if (err) {
          return reject(err);
        }

        resolve();
      });
    }
  );

  try {
    fs.unlinkSync(path.join(workingDirectory, fileName));
  } catch (err) {
    // console.error(err);
  }
}

type ByteProgressCallback = (info: { progress: number, eta: number, speed: string }) => void

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

export async function downloadFile(url: string, fileName: string, workingDirectory: string) {
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
    fs.unlinkSync(path.join(workingDirectory, fileName));
  } catch (err) {
    // console.error(err);
  }

  const bar = cli.progress({
    format: 'Downloading from GitHub [{bar}] {percentage}% | Speed: {speed}',
  });
  bar.start(100, 0);

  const writer = await new Promise<WriteStream>((resolve, reject) => {
    const stream = fs.createWriteStream(path.join(workingDirectory, fileName));

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

export function getTarget(): string {
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

async function fetchRelease() {
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
          await downloadFile(asset.browser_download_url, asset.name, path.resolve(__dirname, '..'));
          await extractFile(asset.name, path.resolve(__dirname, '..'));
        }
      }
    }
  }
}
