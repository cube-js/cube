import decompress from 'decompress';
import fetch, { Headers, Request, Response } from 'node-fetch';
import bytes from 'bytes';
import { throttle } from 'throttle-debounce';
import { SingleBar } from 'cli-progress';
import { mkdirpSync } from 'fs-extra';
import fs from 'fs';
import * as os from 'os';
import crypto from 'crypto';
import * as path from 'path';
import { gunzipSync } from 'zlib';

import { internalExceptions } from './errors';
import { getHttpAgentForProxySettings } from './proxy';

type ByteProgressCallback = (info: { progress: number; eta: number; speed: string }) => void;

export async function streamWithProgress(
  response: Response,
  progressCallback: ByteProgressCallback
): Promise<string> {
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

  const saveFilePath = path.join(os.tmpdir(), crypto.randomBytes(16).toString('hex'));
  const writer = fs.createWriteStream(
    saveFilePath,
  );

  response.body.pipe(writer);
  response.body.on('data', (chunk) => {
    done += chunk.length;
    throttled();
  });

  return new Promise<string>(
    (resolve) => {
      // Wait before writer will finish, because response can be done earlier then extracting
      writer.on('finish', () => {
        resolve(saveFilePath);
      });
    }
  );
}

type DownloadAndExtractFileOptions = {
  showProgress: boolean;
  cwd: string;
};

type DownloadOptions = {
  showProgress: boolean;
  cwd: string;
  filename: string
};

export async function downloadAndExtractFile(url: string, { cwd }: DownloadAndExtractFileOptions) {
  const request = new Request(url, {
    headers: new Headers({
      'Content-Type': 'application/octet-stream',
    }),
    agent: await getHttpAgentForProxySettings(),
  });

  const response = await fetch(request);
  if (!response.ok) {
    throw new Error(`unexpected response ${response.statusText}`);
  }

  const bar = new SingleBar({
    format: 'Downloading [{bar}] {percentage}% | Speed: {speed}',
  });
  bar.start(100, 0);

  try {
    mkdirpSync(cwd);
  } catch (e) {
    internalExceptions(e);
  }

  const savedFilePath = await streamWithProgress(response, ({ progress, speed, eta }) => {
    bar.update(progress, {
      speed,
      eta,
    });
  });

  await decompress(savedFilePath, cwd);

  try {
    fs.unlinkSync(savedFilePath);
  } catch (e) {
    internalExceptions(e);
  }

  bar.stop();
}

export async function download(url: string, { cwd, filename }: DownloadOptions) {
  const request = new Request(url, {
    headers: new Headers({
      'Content-Type': 'application/octet-stream',
    }),
    agent: await getHttpAgentForProxySettings(),
  });

  const response = await fetch(request);
  if (!response.ok) {
    throw new Error(`unexpected response ${response.statusText}`);
  }

  const bar = new SingleBar({
    format: 'Downloading [{bar}] {percentage}% | Speed: {speed}',
  });
  bar.start(100, 0);

  try {
    mkdirpSync(cwd);
  } catch (e) {
    internalExceptions(e);
  }

  const savedFilePath = await streamWithProgress(response, ({ progress, speed, eta }) => {
    bar.update(progress, {
      speed,
      eta,
    });
  });

  const newPath = path.join(cwd, filename);
  fs.renameSync(savedFilePath, newPath);

  try {
    fs.unlinkSync(savedFilePath);
  } catch (e) {
    internalExceptions(e);
  }

  bar.stop();

  return newPath;
}

export async function downloadAndGunzip(url: string): Promise<string> {
  const response = await fetch(url);
  const gz = await response.arrayBuffer();
  const buffer = await gunzipSync(gz);
  return buffer.toString();
}
