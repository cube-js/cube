import * as tar from 'tar';
import extractZip from 'extract-zip';
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

/**
 * Extract a downloaded archive.
 *
 * Dispatches on the archive's **magic bytes**, not its filename. That is not a
 * stylistic choice: `streamWithProgress` saves to
 * `crypto.randomBytes(16).toString('hex')` with no extension at all, so there is
 * nothing to dispatch on by name. The `decompress` package this replaces sniffed
 * content for the same reason.
 *
 * `decompress` is unmaintained and carries two unfixed archive-extraction
 * advisories (GHSA-mp2f-45pm-3cg9, GHSA-h39j-r5qq-r9mm — both "extraction can
 * create files outside the target directory"). Both replacements refuse to
 * escape `cwd`: `tar` strips leading `/` and drops any entry containing `..`,
 * and `extract-zip` resolves each entry against the target and rejects paths
 * that leave it.
 *
 * `.tar.bz2`, which `decompress` also handled, is deliberately not supported —
 * nothing in this repository produces one, and it would mean another dependency.
 * It throws a named error rather than failing obscurely.
 */
export async function extractArchive(archivePath: string, cwd: string): Promise<void> {
  // 262 bytes: enough for the `ustar` magic a plain tar carries at offset 257.
  const header = Buffer.alloc(262);
  const fd = await fs.promises.open(archivePath, 'r');

  let bytesRead: number;

  try {
    ({ bytesRead } = await fd.read(header, 0, header.length, 0));
  } finally {
    await fd.close();
  }

  const startsWith = (...magic: number[]) => bytesRead >= magic.length && magic.every((byte, i) => header[i] === byte);

  // gzip (1f 8b) covers .tar.gz/.tgz; `tar.x` gunzips transparently.
  if (startsWith(0x1f, 0x8b)) {
    await tar.x({ file: archivePath, cwd });
    return;
  }

  // zip: "PK", or "PK" for an empty archive.
  if (startsWith(0x50, 0x4b)) {
    await extractZip(archivePath, { dir: path.resolve(cwd) });
    return;
  }

  // Uncompressed tar: "ustar" at offset 257.
  if (bytesRead >= 262 && header.slice(257, 262).toString('latin1') === 'ustar') {
    await tar.x({ file: archivePath, cwd });
    return;
  }

  if (startsWith(0x42, 0x5a, 0x68)) {
    throw new Error(
      'Unsupported archive format: bzip2. Supported formats are gzip (.tar.gz/.tgz), tar and zip.'
    );
  }

  throw new Error(
    'Unable to detect archive format from its contents. Supported formats are gzip (.tar.gz/.tgz), tar and zip.'
  );
}

type DownloadAndExtractFile = {
  showProgress: boolean;
  cwd: string;
  skipExtract?: boolean;
  dstFileName?: string;
};

export async function downloadAndExtractFile(url: string, { cwd, skipExtract, dstFileName }: DownloadAndExtractFile) {
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
  } catch (e: any) {
    internalExceptions(e);
  }

  const savedFilePath = await streamWithProgress(response, ({ progress, speed, eta }) => {
    bar.update(progress, {
      speed,
      eta,
    });
  });

  if (skipExtract) {
    if (dstFileName) {
      fs.copyFileSync(savedFilePath, path.resolve(path.join(cwd, dstFileName)));
    } else {
      // We still need some name for a file
      const tmpFileName = path.basename(savedFilePath);
      const destPath = path.join(cwd, tmpFileName);
      fs.copyFileSync(savedFilePath, destPath);
    }
  } else {
    await extractArchive(savedFilePath, cwd);
  }

  try {
    fs.unlinkSync(savedFilePath);
  } catch (e: any) {
    internalExceptions(e);
  }

  bar.stop();
}

export async function downloadAndGunzip(url: string): Promise<string> {
  const response = await fetch(url);
  const gz = await response.arrayBuffer();
  const buffer = await gunzipSync(gz);
  return buffer.toString();
}
