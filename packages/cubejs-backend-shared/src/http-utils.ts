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
 * Options shared by every `tar.x` call here.
 *
 * `preserveOwner` defaults to true when running as root, which is the normal case
 * inside the Cube image; extracted files would then take whatever uid/gid the
 * tarball recorded. Writing as the current user matches how this path has always
 * behaved.
 *
 * `onwarn` is load-bearing: tar *drops* unsafe entries with a warning rather than
 * failing, so an archive consisting only of `../evil` would extract to nothing and
 * resolve successfully, leaving the caller to fail later on a confusing
 * missing-file error.
 *
 * Only `TAR_ENTRY_ERROR` — the code tar uses for a rejected path — goes through
 * `internalExceptions`, deliberately: that helper calls `process.exit(1)` under
 * `CUBEJS_INTERNAL_EXCEPTIONS=exit`, and tar also warns about benign conditions
 * (unsupported entry types such as fifos and devices, `TAR_ENTRY_INVALID`, failed
 * utime/chown). Routing those through it would let one odd entry in a third-party
 * tarball take the process down mid-download, where previously it extracted and
 * carried on. Everything else is logged and ignored, as tar itself treats it.
 */
const tarOptions = {
  preserveOwner: false,
  onwarn: (code: string, message: string) => {
    const warning = `tar skipped an entry while extracting (${code}): ${message}`;

    if (code === 'TAR_ENTRY_ERROR') {
      internalExceptions(new Error(warning));

      return;
    }

    console.warn(warning);
  },
};

/**
 * Extract a downloaded archive into `cwd`, which is created if missing.
 *
 * Dispatches on magic bytes, not the filename, because there is no filename to
 * dispatch on: `streamWithProgress` saves downloads as
 * `crypto.randomBytes(16).toString('hex')`, with no extension.
 *
 * Handles gzip (`.tar.gz` / `.tgz`), uncompressed tar and zip. Two gaps are
 * deliberate and both throw a named error rather than failing obscurely: bzip2,
 * and pre-POSIX v7 tars, which carry no `ustar` magic at offset 257 to detect them
 * by.
 *
 * Neither backend writes outside `cwd`: `tar` strips a leading `/` on extraction and
 * drops entries containing `..`, and `extract-zip` rejects entries that resolve outside
 * the target.
 */
export async function extractArchive(archivePath: string, cwd: string): Promise<void> {
  // `extract-zip` creates its target but `tar.x` throws `CwdError` when it is
  // missing, so without this the contract would depend on the archive's format —
  // which callers cannot know in advance, that being the point of magic-byte dispatch.
  mkdirpSync(cwd);

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
    await tar.x({ file: archivePath, cwd, ...tarOptions });
    return;
  }

  // zip: the two-byte "PK" prefix, shared by a local file header and by the
  // end-of-central-directory record that an empty archive consists of.
  if (startsWith(0x50, 0x4b)) {
    await extractZip(archivePath, { dir: path.resolve(cwd) });
    return;
  }

  // Uncompressed tar: "ustar" at offset 257.
  if (bytesRead >= 262 && header.subarray(257, 262).toString('latin1') === 'ustar') {
    await tar.x({ file: archivePath, cwd, ...tarOptions });
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
