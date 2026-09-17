import * as tar from 'tar';
import StreamZip from 'node-stream-zip';
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

const StreamZipAsync = StreamZip.async;

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
 * `preserveOwner` defaults to true when running as root, the normal case inside the
 * Cube image, and extracted files would then take whatever uid/gid the tarball
 * recorded.
 *
 * `onwarn` is load-bearing: tar *drops* unsafe entries with a warning rather than
 * failing, so an archive of nothing but `../evil` would extract to nothing and
 * resolve successfully, leaving the caller on a confusing missing-file error later.
 *
 * Only `TAR_ENTRY_ERROR` is escalated, because `internalExceptions` calls
 * `process.exit(1)` under `CUBEJS_INTERNAL_EXCEPTIONS=exit` and tar also warns about
 * benign conditions (fifos and devices, `TAR_ENTRY_INVALID`, failed utime/chown) —
 * one odd entry in a third-party tarball should not take the process down. tar
 * reports per-entry write failures through `TAR_ENTRY_ERROR` too, not just path
 * rejections, and both mean a half-extracted install.
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
 * A zip records the unix mode in the high half of the external attributes, but only
 * when the high byte of "version made by" is 3 (unix). With the DOS default of 0
 * those same bits are DOS attribute flags, and reading them as a mode would invent
 * file types out of read-only/archive/hidden bits.
 */
function zipEntryUnixMode(entry: StreamZip.ZipEntry): number | undefined {
  // eslint-disable-next-line no-bitwise
  if ((entry.verMade >> 8) !== 3) {
    return undefined;
  }

  // eslint-disable-next-line no-bitwise
  return (entry.attr >>> 16) || undefined;
}

const S_IFMT = 0o170000;
const S_IFLNK = 0o120000;
const S_IXUGO = 0o111;

async function extractZipArchive(archivePath: string, dir: string): Promise<void> {
  // `skipEntryNameValidation` is the default, and passed explicitly because it is what
  // rejects `..`, a leading `/`, a drive letter or a backslash while the central
  // directory is read — before a byte is written. Flip it and Zip Slip is back.
  const zip = new StreamZipAsync({ file: archivePath, skipEntryNameValidation: false });

  try {
    const entries = Object.values(await zip.entries())
      .map((entry) => ({ entry, mode: zipEntryUnixMode(entry) }));

    for (const { entry, mode } of entries) {
      // eslint-disable-next-line no-bitwise
      if (mode !== undefined && (mode & S_IFMT) === S_IFLNK) {
        throw new Error(
          `Refusing to extract "${entry.name}": symlink entries in zip archives are not allowed.`
        );
      }
    }

    await zip.extract(null, dir);

    // `node-stream-zip` opens every file with the default 0o666, where `extract-zip`
    // applied the recorded mode — so a zipped binary would arrive unrunnable. Only the
    // exec bit is restored: honouring the whole mode would let an archive widen its own
    // permissions, and nothing here depends on the rest of it.
    for (const { entry, mode } of entries) {
      // eslint-disable-next-line no-bitwise
      if (entry.isFile && mode !== undefined && (mode & S_IXUGO)) {
        const target = path.join(dir, entry.name);
        const { mode: current } = await fs.promises.stat(target);

        // eslint-disable-next-line no-bitwise
        await fs.promises.chmod(target, current | (mode & S_IXUGO));
      }
    }
  } finally {
    await zip.close();
  }
}

/**
 * Extract a downloaded archive into `cwd`, which is created if missing.
 *
 * Dispatches on magic bytes, not the filename, because there is no filename to
 * dispatch on: `streamWithProgress` saves downloads as
 * `crypto.randomBytes(16).toString('hex')`, with no extension.
 *
 * Two gaps are deliberate, and both throw a named error rather than failing
 * obscurely: bzip2, and pre-POSIX v7 tars, which carry no magic to detect them by.
 *
 * Neither backend writes outside `cwd`: `tar` strips a leading `/` and drops entries
 * containing `..`, and the zip backend rejects such names outright.
 */
export async function extractArchive(archivePath: string, cwd: string): Promise<void> {
  // Neither backend creates its target — `tar.x` throws `CwdError`, the zip backend
  // opens files in a directory it expects to exist — so without this the contract
  // would depend on the archive's format, which callers cannot know in advance.
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

  // gzip covers .tar.gz/.tgz; `tar.x` gunzips transparently.
  if (startsWith(0x1f, 0x8b)) {
    await tar.x({ file: archivePath, cwd, ...tarOptions });
    return;
  }

  // "PK" only: the rest of the signature differs between a local file header and the
  // end-of-central-directory record that an empty archive consists of.
  if (startsWith(0x50, 0x4b)) {
    await extractZipArchive(archivePath, path.resolve(cwd));
    return;
  }

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
