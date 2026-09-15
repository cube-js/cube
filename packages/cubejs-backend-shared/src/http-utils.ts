import * as tar from 'tar';
import * as yauzl from 'yauzl';
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
import { pipeline } from 'stream/promises';

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
 * Only `TAR_ENTRY_ERROR` goes through `internalExceptions`, deliberately: that
 * helper calls `process.exit(1)` under `CUBEJS_INTERNAL_EXCEPTIONS=exit`, and tar
 * also warns about benign conditions (unsupported entry types such as fifos and
 * devices, `TAR_ENTRY_INVALID`, failed utime/chown). Routing those through it
 * would let one odd entry in a third-party tarball take the process down
 * mid-download, where previously it extracted and carried on.
 *
 * `TAR_ENTRY_ERROR` is not only the path-rejection code — measured, tar reports
 * per-entry write failures through it too (a read-only target raises it once per
 * entry, same as a `..` name does). Both belong on this side of the split: a
 * half-extracted install is a real failure, and escalating it is what the opt-in
 * `exit` setting asks for. Everything else is logged and ignored, as tar treats
 * it.
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

// A zip records a unix mode in the high 16 bits of the external attributes; the
// file-type nibble there is what marks an entry a symlink or a directory.
const UNIX_MODE_MASK = 0o170000;
const UNIX_MODE_SYMLINK = 0o120000;
const UNIX_MODE_DIRECTORY = 0o040000;

// eslint-disable-next-line no-bitwise
const unixFileType = (entry: yauzl.Entry) => (entry.externalFileAttributes >>> 16) & UNIX_MODE_MASK;

// 0 when the producer recorded no unix mode at all — a DOS-made zip — where node's
// default is the right answer for both files and directories.
// eslint-disable-next-line no-bitwise
const unixPermissions = (entry: yauzl.Entry) => (entry.externalFileAttributes >>> 16) & 0o777;

/** Pull one entry, or `null` at the end of the archive. */
function nextZipEntry(zipfile: yauzl.ZipFile): Promise<yauzl.Entry | null> {
  return new Promise((resolve, reject) => {
    // `zipfile` outlives a single entry, so every listener has to come back off
    // before settling — three entries in and the handlers would otherwise be
    // stacked three deep, and the first `error` would settle every pending read.
    const cleanups: Array<() => void> = [];
    const settle = (finish: () => void) => {
      cleanups.forEach((off) => off());
      finish();
    };

    const onEntry = (entry: yauzl.Entry) => settle(() => resolve(entry));
    const onEnd = () => settle(() => resolve(null));
    const onError = (err: Error) => settle(() => reject(err));

    cleanups.push(
      () => zipfile.removeListener('entry', onEntry),
      () => zipfile.removeListener('end', onEnd),
      () => zipfile.removeListener('error', onError)
    );

    zipfile.once('entry', onEntry);
    zipfile.once('end', onEnd);
    zipfile.once('error', onError);
    zipfile.readEntry();
  });
}

/**
 * `realpath` of the deepest ancestor of `target` that exists.
 *
 * Any symlink on the path is by definition in the part that already exists, so
 * resolving that prefix accounts for all of them — and doing it before `mkdir` means a
 * rejected entry creates nothing.
 */
async function realpathOfExistingAncestor(target: string): Promise<string> {
  let current = target;

  for (;;) {
    try {
      // eslint-disable-next-line no-await-in-loop
      return await fs.promises.realpath(current);
    } catch (e) {
      // Only "does not exist yet" means keep walking. Anything else — EACCES on an
      // intermediate directory, say — would otherwise approve the entry against a
      // shallower ancestor than the one being checked.
      if ((e as NodeJS.ErrnoException).code !== 'ENOENT') {
        throw e;
      }

      const parent = path.dirname(current);
      if (parent === current) {
        throw e;
      }
      current = parent;
    }
  }
}

type ZipExtraction = {
  zipfile: yauzl.ZipFile;
  /** Resolved target directory; every entry must land inside it. */
  root: string;
  signal: AbortSignal;
  /** Directory modes to apply once every entry is written — see `applyDirectoryModes`. */
  directoryModes: Map<string, number>;
};

/**
 * Apply recorded directory modes once every entry is written — a restrictive mode
 * cannot be set while there are still entries to write underneath it, and `mkdir`
 * will not chmod a directory a child entry already created. Deepest first, because
 * restricting an ancestor takes away the traversal bit its descendants need.
 */
async function applyDirectoryModes(directoryModes: Map<string, number>): Promise<void> {
  const deepestFirst = [...directoryModes.entries()].sort(
    ([a], [b]) => b.split(path.sep).length - a.split(path.sep).length
  );

  // `O_DIRECTORY | O_NOFOLLOW` for the same reason the file path uses `O_NOFOLLOW`:
  // the containment check happened when the entry was seen, and this runs after the
  // whole archive. A link swapped in at `dest` since then would otherwise take an
  // archive-chosen mode outside the target; opening it fails ELOOP instead.
  //
  // Both constants are POSIX-only. On Windows they are `undefined`, which would
  // collapse the flags to a bare `O_RDONLY` and make this an unsupported open-plus-
  // fchmod of a directory — failing *after* the whole archive is already written. Fall
  // back to a plain `chmod` there; a unix-made zip is the only kind that reaches here
  // at all, and Windows symlinks need privilege to create.
  const canOpenDirectory = typeof fs.constants.O_DIRECTORY === 'number'
    && typeof fs.constants.O_NOFOLLOW === 'number';
  // eslint-disable-next-line no-bitwise
  const flags = fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW;

  for (const [dest, mode] of deepestFirst) {
    if (canOpenDirectory) {
      // eslint-disable-next-line no-await-in-loop
      const handle = await fs.promises.open(dest, flags);

      try {
        // eslint-disable-next-line no-await-in-loop
        await handle.chmod(mode);
      } finally {
        // eslint-disable-next-line no-await-in-loop
        await handle.close();
      }
    } else {
      // eslint-disable-next-line no-await-in-loop
      await fs.promises.chmod(dest, mode);
    }
  }
}

async function writeZipEntry(
  { zipfile, root: dir, signal, directoryModes }: ZipExtraction,
  entry: yauzl.Entry
): Promise<void> {
  // Defence in depth. yauzl runs this itself inside `readEntry` while `decodeStrings`
  // is on, so a `..` name errors out of `nextZipEntry` and never reaches here; this
  // keeps the check owned locally rather than by a default we do not set.
  const invalid = yauzl.validateFileName(entry.fileName);
  if (invalid) {
    throw new Error(`Refusing to extract zip entry, ${invalid}`);
  }

  // Refused outright rather than containment-checked (GHSA-jmr9-qjv8-65gv): with no
  // symlink ever created under `dir`, no later entry can resolve out of it either.
  if (unixFileType(entry) === UNIX_MODE_SYMLINK) {
    throw new Error(`Refusing to extract symlink entry from zip: ${entry.fileName}`);
  }

  // Trailing separator stripped before it reaches the filesystem: `path.join` keeps it,
  // and POSIX resolves a trailing slash as if `/.` followed — so `lstat` on `esc/`
  // stats the link's target and reports it is not a link. It also makes
  // `applyDirectoryModes` count a phantom segment when sorting by depth.
  const dest = path.join(dir, entry.fileName.replace(/\/+$/, ''));
  // Lexical first: it costs nothing and refuses a hostile name before any filesystem
  // call. It is not sufficient on its own — see `realpathOfExistingAncestor`.
  if (dest !== dir && !dest.startsWith(dir + path.sep)) {
    throw new Error(`Refusing to extract zip entry out of bound path: ${entry.fileName}`);
  }

  // `.` and `./` are names `validateFileName` accepts, and they normalise to the
  // target itself. The parent of the root is outside the root by construction, so
  // without this the check below reads them as an escape and aborts the archive.
  if (dest === dir) {
    return;
  }

  const parent = await realpathOfExistingAncestor(path.dirname(dest));
  if (parent !== dir && !parent.startsWith(dir + path.sep)) {
    throw new Error(`Refusing to extract zip entry out of bound path: ${entry.fileName}`);
  }

  // Resolving the parent cannot see the last component: a link there is written
  // *through*, and a dangling one has its target created by the open. `lstat` is the
  // only check that sees both, and it has to come before the directory branch, which
  // would otherwise `mkdir` through the link and chmod a directory outside `dir`.
  const existing = await fs.promises.lstat(dest).catch(() => null);
  if (existing?.isSymbolicLink()) {
    throw new Error(`Refusing to extract zip entry over a symlink: ${entry.fileName}`);
  }

  // The trailing slash is the convention, but a producer may mark a directory by mode
  // alone; read as a file, it lands as an empty regular file and the first entry under
  // it collides on `mkdir` with EEXIST.
  if (entry.fileName.endsWith('/') || unixFileType(entry) === UNIX_MODE_DIRECTORY) {
    await fs.promises.mkdir(dest, { recursive: true });

    // Recorded now, applied after the last entry: a `0o700` directory has to end up
    // `0o700` rather than inheriting the umask, but it cannot be restricted while
    // there are still entries to write under it. The root cannot reach here — an entry
    // naming it returns above — so an archive can never set the caller's own mode.
    const dirMode = unixPermissions(entry);
    if (dirMode) {
      directoryModes.set(dest, dirMode);
    }
    return;
  }

  await fs.promises.mkdir(path.dirname(dest), { recursive: true });

  const mode = unixPermissions(entry);
  const readStream = await zipfile.openReadStreamPromise(entry);

  // `O_NOFOLLOW` rather than trusting the `lstat` above: that is a check-then-open
  // race, and this makes the write refuse a link on its own terms (ELOOP). Opened by
  // hand because `createWriteStream`'s `flags` is typed as a string. On Windows the
  // constant is `undefined` and the flag is silently dropped, leaving the `lstat` as
  // the only guard — acceptable there, where creating a symlink needs privilege.
  // eslint-disable-next-line no-bitwise
  const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_TRUNC | fs.constants.O_NOFOLLOW;

  let handle: fs.promises.FileHandle;

  try {
    handle = await fs.promises.open(dest, flags, mode || undefined);
  } catch (e) {
    // Nothing else will consume `readStream`, and yauzl only unrefs the archive's
    // descriptor when the entry stream ends or is destroyed — so without this an
    // ELOOP here (the case `O_NOFOLLOW` exists to produce) leaks the archive's fd.
    readStream.destroy();
    throw e;
  }

  // The signal is what tears these two down when the zipfile errors out from under
  // them — losing the race only abandons this promise, it does not close anything.
  // The handle closes with the stream it was turned into.
  await pipeline(readStream, handle.createWriteStream(), { signal });
}

/**
 * Extract a zip into `dir`, which must already exist.
 */
async function extractZipArchive(archivePath: string, dir: string): Promise<void> {
  // Before the zipfile is opened: a throw here would otherwise leak its descriptor,
  // which nothing closes until the `finally` further down. Resolved because it is
  // compared against resolved parents below — on macOS a caller's `/tmp/...` is
  // already a symlink to `/private/tmp/...`.
  const root = await fs.promises.realpath(dir);

  const zipfile = await yauzl.openPromise(archivePath, { lazyEntries: true });

  // yauzl reports reader failures by emitting `error` on the zipfile, and an emit with
  // no listener *throws* — `nextZipEntry`'s comes off between reads, so this one has to
  // stay on for the zipfile's lifetime.
  let raiseFatal!: (err: Error) => void;
  const fatal = new Promise<never>((_resolve, reject) => {
    raiseFatal = reject;
  });
  // An error arriving after the last read has no race left to observe it.
  fatal.catch(() => undefined);

  const aborter = new AbortController();
  zipfile.on('error', (err: Error) => {
    // Settle the race first: `abort()` dispatches synchronously, and losing to
    // `pipeline`'s `AbortError` would swallow the reader's real error.
    raiseFatal(err);
    aborter.abort();
  });

  const extraction: ZipExtraction = {
    zipfile,
    root,
    signal: aborter.signal,
    directoryModes: new Map(),
  };

  try {
    for (;;) {
      // Sequential on purpose: entries are read from one cursor.
      // eslint-disable-next-line no-await-in-loop
      const entry = await Promise.race([nextZipEntry(zipfile), fatal]);
      if (!entry) {
        break;
      }
      // Raced, not checked after: a failure can leave the read stream neither ending
      // nor erroring, and `pipeline` would then never settle.
      // eslint-disable-next-line no-await-in-loop
      await Promise.race([writeZipEntry(extraction, entry), fatal]);
    }

    await applyDirectoryModes(extraction.directoryModes);
  } finally {
    zipfile.close();
  }
}

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
 * drops entries containing `..`, and the zip backend rejects absolute and `..` names
 * outright and refuses symlink entries altogether.
 */
export async function extractArchive(archivePath: string, cwd: string): Promise<void> {
  // The zip backend needs its target to exist and `tar.x` throws `CwdError` when it is
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
    await extractZipArchive(archivePath, path.resolve(cwd));
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
