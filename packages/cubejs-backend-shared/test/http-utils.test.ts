import fs from 'fs';
import os from 'os';
import path from 'path';
import * as tar from 'tar';
import { crc32 } from 'zlib';

import { PassThrough, Readable, Writable } from 'stream';
import * as yauzl from 'yauzl';

import { extractArchive } from '../src/http-utils';

// The module namespace object is frozen under the ESM interop, so `jest.spyOn` cannot
// redefine `openPromise`. Replace the module with a passthrough whose one export is a
// mock, which a single test swaps out to hand back a zipfile it can make misbehave.
jest.mock('yauzl', () => {
  const actual = jest.requireActual<typeof import('yauzl')>('yauzl');
  return { ...actual, openPromise: jest.fn(actual.openPromise) };
});

/**
 * `extractArchive` replaced the unmaintained `decompress`, which carries two
 * unfixed advisories — GHSA-mp2f-45pm-3cg9 ("archive extraction can create files
 * and links outside of the target directory") and GHSA-h39j-r5qq-r9mm (Zip Slip).
 *
 * These tests exist to prove the replacement is not vulnerable to the same class,
 * so they build genuinely hostile archives rather than asserting on library
 * version numbers. They also cover the happy paths, because dispatch is by magic
 * bytes: `streamWithProgress` saves downloads under a random hex name with no
 * extension, so there is nothing to dispatch on by filename.
 */
describe('extractArchive', () => {
  let work: string;

  beforeEach(() => {
    work = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'extract-archive-'));
  });

  afterEach(() => {
    fs.rmSync(work, { recursive: true, force: true });
  });

  /** Teardown after an abort is asynchronous, so poll rather than assert on the next tick. */
  const waitUntil = async (condition: () => boolean, what: string, timeoutMs = 2000) => {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
      if (Date.now() > deadline) {
        throw new Error(`Timed out waiting for ${what}`);
      }
      // eslint-disable-next-line no-await-in-loop
      await new Promise((resolve) => { setTimeout(resolve, 10); });
    }
  };

  const targetDir = () => {
    const dir = path.join(work, 'target');
    fs.mkdirSync(dir, { recursive: true });
    return dir;
  };

  /**
   * Assert the fixture really is hostile before extracting it.
   *
   * The zip fixtures are safe by construction — the rejection itself proves the
   * hostile name survived into the archive. The tar fixtures have no such witness:
   * absolute-path stripping already happens in tar's `WriteEntry` constructor, and
   * only ordering keeps the `..` name assigned in `onWriteEntry` intact. If a future
   * tar normalises it, the fixture silently becomes benign and these tests keep
   * passing while proving nothing — the exact trap the zip fixture is hand-rolled to
   * avoid. So read the names back.
   */
  const storedNames = async (archive: string) => {
    const names: string[] = [];
    await tar.t({ file: archive, onReadEntry: (e) => names.push(e.path) });
    return names;
  };

  /**
   * Build a .zip with entry names stored verbatim.
   *
   * Hand-rolled (stored/uncompressed, so no deflate needed) rather than using a
   * zip library, because every maintained writer *sanitises* what it stores:
   * `archiver` silently rewrites `../ZIP_PWNED.txt` to `ZIP_PWNED.txt`, which
   * would make the Zip Slip test below extract a perfectly benign archive and
   * pass for the wrong reason. Byte control is the point.
   */
  const writeZip = async (file: string, entries: { name: string; content: string; mode?: number }[]) => {
    const local: Buffer[] = [];
    const central: Buffer[] = [];
    let offset = 0;

    for (const entry of entries) {
      const name = Buffer.from(entry.name, 'utf8');
      const data = Buffer.from(entry.content, 'utf8');
      const sum = crc32(data);

      const lfh = Buffer.alloc(30);
      lfh.writeUInt32LE(0x04034b50, 0); // local file header signature
      lfh.writeUInt16LE(10, 4); // version needed
      lfh.writeUInt16LE(0, 8); // method: stored
      lfh.writeUInt32LE(sum, 14);
      lfh.writeUInt32LE(data.length, 18); // compressed size
      lfh.writeUInt32LE(data.length, 22); // uncompressed size
      lfh.writeUInt16LE(name.length, 26);
      local.push(lfh, name, data);

      const cdh = Buffer.alloc(46);
      cdh.writeUInt32LE(0x02014b50, 0); // central directory signature
      // version made by: high byte is the host system. 3 = unix, which is what a
      // producer capable of recording a symlink emits — with the default 0 (MS-DOS)
      // the external-attributes field is formally DOS attribute bits and the unix
      // mode below is not meant to be read at all.
      // eslint-disable-next-line no-bitwise
      cdh.writeUInt16LE((3 << 8) | 20, 4);
      cdh.writeUInt16LE(10, 6); // version needed
      cdh.writeUInt16LE(0, 10); // method: stored
      cdh.writeUInt32LE(sum, 16);
      cdh.writeUInt32LE(data.length, 20);
      cdh.writeUInt32LE(data.length, 24);
      cdh.writeUInt16LE(name.length, 28);
      // External attributes carry the unix mode in the high 16 bits, which is how a
      // zip records a symlink (`0o120000`). `>>> 0` because the shift overflows into a
      // negative signed int32 otherwise.
      // eslint-disable-next-line no-bitwise
      cdh.writeUInt32LE((((entry.mode ?? 0o100644) << 16) >>> 0), 38);
      cdh.writeUInt32LE(offset, 42); // relative offset of local header
      central.push(cdh, name);

      offset += lfh.length + name.length + data.length;
    }

    const centralBuf = Buffer.concat(central);
    const eocd = Buffer.alloc(22);
    eocd.writeUInt32LE(0x06054b50, 0); // end of central directory signature
    eocd.writeUInt16LE(entries.length, 8);
    eocd.writeUInt16LE(entries.length, 10);
    eocd.writeUInt32LE(centralBuf.length, 12);
    eocd.writeUInt32LE(offset, 16);

    await fs.promises.writeFile(file, Buffer.concat([...local, centralBuf, eocd]));
  };

  /** Build a .tar.gz whose entries we control byte-for-byte, including hostile names. */
  const writeTarGz = async (file: string, entries: { name: string; content?: string; symlinkTo?: string }[]) => {
    const stage = fs.mkdtempSync(path.join(work, 'stage-'));
    const names: string[] = [];

    for (const entry of entries) {
      // Stage under a safe name, then rewrite the stored name via tar's own API.
      const safe = `entry-${names.length}`;
      if (entry.symlinkTo !== undefined) {
        fs.symlinkSync(entry.symlinkTo, path.join(stage, safe));
      } else {
        fs.writeFileSync(path.join(stage, safe), entry.content ?? '');
      }
      names.push(safe);
    }

    await tar.c(
      {
        file,
        gzip: true,
        cwd: stage,
        portable: true,
        onWriteEntry(e) {
          const idx = names.indexOf(e.path);
          if (idx >= 0) {
            // eslint-disable-next-line no-param-reassign
            e.path = entries[idx].name;
          }
        },
      },
      names
    );
  };

  describe('refuses to write outside the target directory', () => {
    it('drops a tar entry that traverses up with ..', async () => {
      const archive = path.join(work, 'evil.tar.gz');
      await writeTarGz(archive, [
        { name: '../PWNED.txt', content: 'pwned' },
        // A benign sibling, so a pass distinguishes "dropped the bad entry" from
        // "extracted nothing at all".
        { name: 'safe.txt', content: 'safe' },
      ]);

      expect(await storedNames(archive)).toContain('../PWNED.txt');

      const target = targetDir();
      await extractArchive(archive, target);

      expect(fs.existsSync(path.join(work, 'PWNED.txt'))).toBe(false);
      expect(fs.readFileSync(path.join(target, 'safe.txt'), 'utf8')).toBe('safe');
    });

    it('contains a tar entry with an absolute path instead of honouring it', async () => {
      const archive = path.join(work, 'abs.tar.gz');
      const escapeTo = path.join(work, 'ABS_PWNED.txt');
      await writeTarGz(archive, [{ name: escapeTo, content: 'pwned' }]);

      // The absolute name survives verbatim into the archive — tar strips the leading
      // `/` when *extracting*, not when writing — so the fixture really is hostile.
      expect(await storedNames(archive)).toContain(escapeTo);

      const target = targetDir();
      await extractArchive(archive, target);

      // tar strips the leading `/` rather than writing to the absolute location, so
      // the entry lands *inside* the target, re-rooted at its otherwise-unchanged
      // path. Assert that positively: "nothing escaped" alone cannot distinguish
      // contained from dropped.
      expect(fs.existsSync(escapeTo)).toBe(false);
      expect(fs.existsSync(path.join(target, escapeTo))).toBe(true);
    });

    it('rejects a zip entry that traverses up with .. (Zip Slip)', async () => {
      const archive = path.join(work, 'evil.zip');
      await writeZip(archive, [{ name: '../ZIP_PWNED.txt', content: 'pwned' }]);

      await expect(extractArchive(archive, targetDir())).rejects.toThrow(/invalid relative path/i);
      expect(fs.existsSync(path.join(work, 'ZIP_PWNED.txt'))).toBe(false);
    });

    it('does not follow a tar symlink that points outside the target', async () => {
      const archive = path.join(work, 'sym.tar.gz');
      const outside = path.join(work, 'outside');
      fs.mkdirSync(outside);

      await writeTarGz(archive, [
        { name: 'esc', symlinkTo: outside },
        { name: 'esc/SYM_PWNED.txt', content: 'pwned' },
      ]);

      expect(await storedNames(archive)).toEqual(
        expect.arrayContaining(['esc', 'esc/SYM_PWNED.txt'])
      );

      // Either it refuses the entry or it writes inside the target; it must not
      // materialise a file in `outside`.
      await extractArchive(archive, targetDir()).catch(() => undefined);

      expect(fs.existsSync(path.join(outside, 'SYM_PWNED.txt'))).toBe(false);
    });

    it('refuses a zip symlink entry outright, rather than the write through it', async () => {
      // A symlink entry has a clean relative *name*, so a name check waves it through
      // and the next entry is written through it. Catching only that second entry
      // still leaves an attacker-controlled link pointing out of the target, so assert
      // the link itself never appears.
      const archive = path.join(work, 'zipsym.zip');
      const outside = path.join(work, 'outside');
      fs.mkdirSync(outside);

      await writeZip(archive, [
        { name: 'esc', content: outside, mode: 0o120777 },
        { name: 'esc/PWNED.txt', content: 'pwned-through-symlink' },
      ]);

      const target = targetDir();
      await expect(extractArchive(archive, target)).rejects.toThrow(/symlink/i);

      expect(fs.existsSync(path.join(outside, 'PWNED.txt'))).toBe(false);
      expect(fs.existsSync(path.join(target, 'esc'))).toBe(false);
    });

    it('refuses a zip entry whose parent is a symlink that was already there', async () => {
      // The symlink refusal above only covers links *this* extraction would create.
      // `downloadAndExtractFile` takes a caller-supplied `cwd` that is not required to
      // be empty, and the tar backend does write symlink entries — so tar first, zip
      // second into the same directory puts an escaping link in the path of a zip entry
      // with a blameless relative name. A lexical containment check cannot see it.
      const archive = path.join(work, 'preexisting.zip');
      // Nested one deeper than the link so the refusal also has to happen *before*
      // `mkdir`: resolving only after creating the parent would leave `outside/sub`
      // behind on the way to rejecting.
      await writeZip(archive, [{ name: 'esc/sub/PWNED.txt', content: 'pwned-through-preexisting' }]);

      const outside = path.join(work, 'outside');
      fs.mkdirSync(outside);
      const target = targetDir();
      fs.symlinkSync(outside, path.join(target, 'esc'));

      await expect(extractArchive(archive, target)).rejects.toThrow(/out of bound path/i);
      expect(fs.existsSync(path.join(outside, 'sub'))).toBe(false);
      expect(fs.existsSync(path.join(outside, 'sub', 'PWNED.txt'))).toBe(false);
    });

    it('treats a `./` root entry as a no-op instead of an escape, and keeps the target\'s mode', async () => {
      // `validateFileName` accepts a bare `.`, and `'./'` strips to it — both normalise
      // to the target itself. Some jar-adjacent packagers emit one, so reading it as an
      // escape would turn a working driver download into a hard failure; and the mode
      // it carries belongs to the caller, not to the archive.
      const archive = path.join(work, 'dotdir.zip');
      await writeZip(archive, [
        { name: './', content: '', mode: 0o040777 },
        { name: 'driver.txt', content: 'legit-content' },
      ]);

      const target = targetDir();
      // eslint-disable-next-line no-bitwise
      const before = (fs.statSync(target).mode & 0o777).toString(8);

      await extractArchive(archive, target);

      expect(fs.readFileSync(path.join(target, 'driver.txt'), 'utf8')).toBe('legit-content');
      // eslint-disable-next-line no-bitwise
      expect((fs.statSync(target).mode & 0o777).toString(8)).toBe(before);
    });

    it('refuses a trailing-slash entry over a pre-existing symlink', async () => {
      // `path.join` keeps the trailing separator and POSIX resolves it as if `/.`
      // followed, so `lstat('…/esc/')` stats the link's target and reports "not a
      // link" — the one name shape that reaches the directory branch past that guard.
      // Left unstripped, `mkdir` resolves through the link and the deferred pass
      // chmods a directory outside the target.
      const archive = path.join(work, 'slashdir.zip');
      await writeZip(archive, [{ name: 'esc/', content: '', mode: 0o040777 }]);

      const outside = path.join(work, 'outside');
      fs.mkdirSync(outside, { mode: 0o755 });
      const target = targetDir();
      fs.symlinkSync(outside, path.join(target, 'esc'));

      await expect(extractArchive(archive, target)).rejects.toThrow(/symlink/i);
      // eslint-disable-next-line no-bitwise
      expect((fs.statSync(outside).mode & 0o777).toString(8)).toBe('755');
    });

    it('refuses to write through a symlink already standing at the entry name', async () => {
      // Resolving the parent leaves the last component unchecked, so this is the same
      // escape one level shallower: the entry is named exactly like the link.
      const archive = path.join(work, 'overlink.zip');
      await writeZip(archive, [{ name: 'esc', content: 'overwritten' }]);

      const outside = path.join(work, 'outside');
      fs.mkdirSync(outside);
      const secret = path.join(outside, 'secret');
      fs.writeFileSync(secret, 'original');

      const target = targetDir();
      fs.symlinkSync(secret, path.join(target, 'esc'));

      await expect(extractArchive(archive, target)).rejects.toThrow(/over a symlink/i);
      expect(fs.readFileSync(secret, 'utf8')).toBe('original');
    });

    it('refuses a dangling symlink at the entry name, which would create its target', async () => {
      // `realpath` cannot see this one at all — it throws ENOENT and resolves to an
      // ancestor — yet `open(…, 'w')` through the link creates the file outside.
      const archive = path.join(work, 'danglinglink.zip');
      await writeZip(archive, [{ name: 'esc', content: 'created-outside' }]);

      const outside = path.join(work, 'outside');
      fs.mkdirSync(outside);
      const notYetThere = path.join(outside, 'new');

      const target = targetDir();
      fs.symlinkSync(notYetThere, path.join(target, 'esc'));

      await expect(extractArchive(archive, target)).rejects.toThrow(/over a symlink/i);
      expect(fs.existsSync(notYetThere)).toBe(false);
    });
  });

  describe('does not leak the archive descriptor when an entry fails', () => {
    it('releases it when opening the destination throws', async () => {
      // `openReadStreamPromise` refs the archive's reader, and yauzl only unrefs on the
      // entry stream's end or destroy — so a rejection between those two calls leaves
      // `zipfile.close()` unrefing against a count the abandoned stream still holds.
      // A directory entry followed by a file of the same name makes the open throw
      // EISDIR, which is that window without needing a race.
      const archive = path.join(work, 'eisdir.zip');
      await writeZip(archive, [
        { name: 'clash/', content: '', mode: 0o040755 },
        { name: 'clash', content: 'x' },
      ]);

      // Witnessed on the stream itself rather than by counting `/dev/fd`: that is
      // POSIX-only and moves with anything else the worker happens to hold open, so a
      // flake would point at the production code instead of at the measurement.
      const { openPromise } = jest.requireActual<typeof import('yauzl')>('yauzl');
      const entryStreams: Readable[] = [];

      (yauzl.openPromise as jest.MockedFunction<typeof yauzl.openPromise>).mockImplementationOnce(
        async (file: string, options?: yauzl.Options) => {
          const zipfile = await openPromise(file, options);
          const openReadStreamPromise = zipfile.openReadStreamPromise.bind(zipfile);

          zipfile.openReadStreamPromise = async (...args: Parameters<typeof openReadStreamPromise>) => {
            const stream = await openReadStreamPromise(...args);
            entryStreams.push(stream);
            return stream;
          };

          return zipfile;
        }
      );

      await expect(extractArchive(archive, targetDir())).rejects.toThrow(/EISDIR/);

      expect(entryStreams).toHaveLength(1);
      await waitUntil(
        () => entryStreams[0].destroyed,
        "the abandoned entry stream to be destroyed, which is what unrefs the archive's descriptor"
      );
    });

    it('releases it when the mode fixup after the open throws', async () => {
      // The open's own catch does not cover what follows it: `umaskAllowed`'s probe and
      // the `chmod` both run before `pipeline` exists to tear anything down, and either
      // can fail on a real filesystem (EACCES probing a read-only `cwd`, EPERM
      // chmodding a file a previous run left under another uid).
      const archive = path.join(work, 'chmodfail.zip');
      await writeZip(archive, [{ name: 'driver.jar', content: 'new', mode: 0o100644 }]);

      const target = targetDir();
      fs.writeFileSync(path.join(target, 'driver.jar'), 'old');

      const { openPromise } = jest.requireActual<typeof import('yauzl')>('yauzl');
      const entryStreams: Readable[] = [];

      (yauzl.openPromise as jest.MockedFunction<typeof yauzl.openPromise>).mockImplementationOnce(
        async (file: string, options?: yauzl.Options) => {
          const zipfile = await openPromise(file, options);
          const openReadStreamPromise = zipfile.openReadStreamPromise.bind(zipfile);

          zipfile.openReadStreamPromise = async (...args: Parameters<typeof openReadStreamPromise>) => {
            const stream = await openReadStreamPromise(...args);
            entryStreams.push(stream);
            return stream;
          };

          return zipfile;
        }
      );

      const open = fs.promises.open.bind(fs.promises);
      jest.spyOn(fs.promises, 'open').mockImplementation(async (...args: Parameters<typeof fs.promises.open>) => {
        const handle = await open(...args);
        handle.chmod = async () => {
          throw Object.assign(new Error('EPERM: operation not permitted, fchmod'), { code: 'EPERM' });
        };
        return handle;
      });

      try {
        await expect(extractArchive(archive, target)).rejects.toThrow(/EPERM/);

        expect(entryStreams).toHaveLength(1);
        await waitUntil(() => entryStreams[0].destroyed, 'the abandoned entry stream to be destroyed');
      } finally {
        jest.restoreAllMocks();
      }
    });
  });

  describe('survives a reader failure rather than crashing the process', () => {
    it('rejects while the entry is still being written, not once the write settles', async () => {
      // The read stream never ends and never errors, so `pipeline` never settles and the
      // zipfile's `error` is the only signal there is — which is what makes this pin
      // racing the write rather than checking after it.
      //
      // The error is scheduled from an `entry` listener registered before
      // `nextZipEntry`'s, so it lands after that per-read listener has come off again.
      const archive = path.join(work, 'two-entries.zip');
      await writeZip(archive, [
        { name: 'a.txt', content: 'first' },
        { name: 'b.txt', content: 'second' },
      ]);

      const { openPromise } = jest.requireActual<typeof import('yauzl')>('yauzl');
      const stalled = new PassThrough();

      (yauzl.openPromise as jest.MockedFunction<typeof yauzl.openPromise>).mockImplementationOnce(
        async (file: string, options?: yauzl.Options) => {
          const zipfile = await openPromise(file, options);

          zipfile.openReadStreamPromise = async () => stalled;

          zipfile.once('entry', () => {
            setImmediate(() => zipfile.emit('error', new Error('reader exploded')));
          });

          return zipfile;
        }
      );

      // Losing the race only abandons the write promise — nothing in `Promise.race`
      // closes what it was doing, so capture the destination stream and require that
      // the abort actually tore it down.
      // Captured through `fs.promises.open`, since the entry is opened by hand for
      // `O_NOFOLLOW` and turned into a stream off the handle.
      const open = fs.promises.open.bind(fs.promises);
      const opened: Writable[] = [];
      jest.spyOn(fs.promises, 'open').mockImplementation(async (...args: Parameters<typeof fs.promises.open>) => {
        const handle = await open(...args);
        const createWriteStream = handle.createWriteStream.bind(handle);

        handle.createWriteStream = (...streamArgs: Parameters<typeof handle.createWriteStream>) => {
          const stream = createWriteStream(...streamArgs);
          opened.push(stream);
          return stream;
        };

        return handle;
      });

      try {
        await expect(extractArchive(archive, targetDir())).rejects.toThrow(/reader exploded/);

        // The rejection outruns the write, which is the point of racing — so wait for
        // the abandoned write to reach its destination stream before asking whether
        // anything closed it.
        await waitUntil(() => opened.length === 1, 'the destination stream to be created');
        await waitUntil(() => opened[0].destroyed, 'the destination stream to be destroyed');
        // The source too: `pipeline` is entered with the signal already aborted here,
        // and destroying the entry stream is what unrefs the archive's descriptor.
        await waitUntil(() => stalled.destroyed, 'the entry stream to be destroyed');
      } finally {
        jest.restoreAllMocks();
      }
    });
  });

  describe('extracts the formats the previous implementation supported', () => {
    it('detects gzip from magic bytes and extracts a .tar.gz', async () => {
      const archive = path.join(work, 'good.tar.gz');
      await writeTarGz(archive, [{ name: 'dir/file.txt', content: 'legit-content' }]);

      const target = targetDir();
      await extractArchive(archive, target);

      expect(fs.readFileSync(path.join(target, 'dir', 'file.txt'), 'utf8')).toBe('legit-content');
    });

    it('detects a zip from the PK magic and extracts it', async () => {
      const archive = path.join(work, 'good.zip');
      await writeZip(archive, [{ name: 'dir/file.txt', content: 'legit-content' }]);

      const target = targetDir();
      await extractArchive(archive, target);

      expect(fs.readFileSync(path.join(target, 'dir', 'file.txt'), 'utf8')).toBe('legit-content');
    });

    it('treats a zip entry marked a directory by mode alone as a directory', async () => {
      // The trailing slash is the convention, not the rule. Read as a file, this entry
      // lands as an empty regular file and the entry under it collides on `mkdir`.
      const archive = path.join(work, 'moddir.zip');
      await writeZip(archive, [
        { name: 'plugins', content: '', mode: 0o040755 },
        { name: 'plugins/driver.txt', content: 'legit-content' },
      ]);

      const target = targetDir();
      await extractArchive(archive, target);

      expect(fs.statSync(path.join(target, 'plugins')).isDirectory()).toBe(true);
      expect(fs.readFileSync(path.join(target, 'plugins', 'driver.txt'), 'utf8')).toBe('legit-content');
    });

    it('keeps an entry\'s exec bit, and leaves a mode-less entry to node\'s default', async () => {
      // Both sides of `mode || undefined` in one fixture. A launcher that
      // extracts as 0644 fails at exec time, far from here; and a DOS-made zip records
      // no unix mode at all, where passing the 0 through would make the file unreadable.
      const archive = path.join(work, 'modes.zip');
      await writeZip(archive, [
        { name: 'bin/run.sh', content: '#!/bin/sh\n', mode: 0o100755 },
        { name: 'dos.txt', content: 'x', mode: 0 },
      ]);

      const target = targetDir();
      await extractArchive(archive, target);

      // eslint-disable-next-line no-bitwise
      expect(fs.statSync(path.join(target, 'bin', 'run.sh')).mode & 0o111).not.toBe(0);
      expect(fs.readFileSync(path.join(target, 'dos.txt'), 'utf8')).toBe('x');
    });

    it('applies a directory mode recorded after its own children', async () => {
      // Nothing in the zip format orders a directory entry before its contents, and
      // `mkdir({ recursive: true })` will not chmod one a child entry already made.
      const archive = path.join(work, 'dirlate.zip');
      await writeZip(archive, [
        { name: 'private/file.txt', content: 'x' },
        { name: 'private', content: '', mode: 0o040700 },
      ]);

      const target = targetDir();
      await extractArchive(archive, target);

      // eslint-disable-next-line no-bitwise
      expect((fs.statSync(path.join(target, 'private')).mode & 0o777).toString(8)).toBe('700');
      expect(fs.readFileSync(path.join(target, 'private', 'file.txt'), 'utf8')).toBe('x');
    });

    it('writes into a directory the archive marks unwritable, then restricts it', async () => {
      // A `0o500` directory applied at `mkdir` time makes every later write under it
      // fail EACCES for a non-root user — so the mode has to land after the contents.
      const archive = path.join(work, 'dirreadonly.zip');
      await writeZip(archive, [
        { name: 'locked', content: '', mode: 0o040500 },
        { name: 'locked/file.txt', content: 'x' },
      ]);

      const target = targetDir();
      await extractArchive(archive, target);

      const locked = path.join(target, 'locked');
      // eslint-disable-next-line no-bitwise
      const mode = (fs.statSync(locked).mode & 0o777).toString(8);
      const written = fs.readFileSync(path.join(locked, 'file.txt'), 'utf8');

      // Before the assertions, not after: a failure would otherwise leave a 0o500
      // directory for `afterEach`'s rm to trip over, and its EACCES would be what
      // surfaces instead of the assertion that actually failed.
      fs.chmodSync(locked, 0o700);

      expect(mode).toBe('500');
      // Root ignores the missing write bit, so the EACCES half of this only exists for
      // a non-root writer — under root the pre-deferral code passes here too.
      if (process.getuid?.() !== 0) {
        expect(written).toBe('x');
      }
    });

    it('filters a directory mode through the umask, as the file path already is', async () => {
      // `chmod` sets bits verbatim where `open` filters them, so without masking an
      // archive gets to choose a world-writable directory under the extraction target —
      // somewhere Cube later loads code from.
      const archive = path.join(work, 'worldwritable.zip');
      await writeZip(archive, [
        { name: 'plugins', content: '', mode: 0o040777 },
        { name: 'plugins/driver.txt', content: 'x' },
      ]);

      const target = targetDir();
      // Pre-created wide, which is the case a stat of the extracted directory cannot
      // mask: `0o777 & ~umask` only describes a directory `mkdir` actually made, and
      // `downloadAndExtractFile`'s `cwd` is not required to be empty.
      fs.mkdirSync(path.join(target, 'plugins'), { mode: 0o777 });
      fs.chmodSync(path.join(target, 'plugins'), 0o777);

      // Pinned rather than measured: computing the expectation from the live umask
      // makes the test agree with itself on a host with `umask 000` — a root container
      // — where it would then assert nothing, or fail for a reason unrelated to the
      // code. Restored in `finally`, or it leaks into every later test in the worker.
      const previousUmask = process.umask(0o022);

      try {
        await extractArchive(archive, target);
      } finally {
        process.umask(previousUmask);
      }

      // eslint-disable-next-line no-bitwise
      expect((fs.statSync(path.join(target, 'plugins')).mode & 0o777).toString(8)).toBe('755');
      expect(fs.readdirSync(target).filter((e) => e.startsWith('.cube-umask-probe-'))).toEqual([]);
    });

    it('narrows a pre-existing file to the mode the archive records', async () => {
      // `O_CREAT` sets the mode only on a file it creates and `O_TRUNC` leaves an
      // existing one's bits alone, so without an explicit chmod a re-extraction over a
      // wide file reports success and leaves it wide.
      const archive = path.join(work, 'overwrite.zip');
      await writeZip(archive, [{ name: 'driver.jar', content: 'new', mode: 0o100644 }]);

      const target = targetDir();
      fs.writeFileSync(path.join(target, 'driver.jar'), 'old');
      fs.chmodSync(path.join(target, 'driver.jar'), 0o666);

      await extractArchive(archive, target);

      expect(fs.readFileSync(path.join(target, 'driver.jar'), 'utf8')).toBe('new');
      // eslint-disable-next-line no-bitwise
      expect((fs.statSync(path.join(target, 'driver.jar')).mode & 0o777).toString(8)).toBe('644');
    });

    it('masks without probing when the archive created a directory of its own', async () => {
      // The umask is read off a directory `mkdir` reported creating, so the common
      // archive needs no probe — and leaves nothing behind in the caller's directory.
      const archive = path.join(work, 'createdmask.zip');
      await writeZip(archive, [
        { name: 'plugins', content: '', mode: 0o040777 },
        { name: 'plugins/driver.txt', content: 'x' },
      ]);

      const target = targetDir();
      // Asserted on the `mkdir` calls, not on what is left on disk: the probe removes
      // itself, so a leftover check passes whether or not one was ever made.
      const mkdir = fs.promises.mkdir.bind(fs.promises);
      const madeDirectories: string[] = [];
      jest.spyOn(fs.promises, 'mkdir').mockImplementation((...args: Parameters<typeof fs.promises.mkdir>) => {
        madeDirectories.push(String(args[0]));
        return mkdir(...args);
      });

      const previousUmask = process.umask(0o022);

      try {
        await extractArchive(archive, target);
      } finally {
        process.umask(previousUmask);
        jest.restoreAllMocks();
      }

      // eslint-disable-next-line no-bitwise
      expect((fs.statSync(path.join(target, 'plugins')).mode & 0o777).toString(8)).toBe('755');
      expect(madeDirectories.filter((d) => d.includes('.cube-umask-probe-'))).toEqual([]);
    });

    it('keeps a directory entry\'s mode too, not just a file\'s', async () => {
      // Directories go through `mkdir`, which takes its own mode — so this is a
      // separate code path from the file bits above, and dropping it silently widens
      // a private directory to group- and other-readable.
      const archive = path.join(work, 'dirmode.zip');
      await writeZip(archive, [{ name: 'private', content: '', mode: 0o040700 }]);

      const target = targetDir();
      await extractArchive(archive, target);

      const stat = fs.statSync(path.join(target, 'private'));
      expect(stat.isDirectory()).toBe(true);
      // eslint-disable-next-line no-bitwise
      expect((stat.mode & 0o777).toString(8)).toBe('700');
    });

    it('detects an uncompressed tar from the ustar magic at offset 257', async () => {
      const stage = fs.mkdtempSync(path.join(work, 'plain-'));
      fs.mkdirSync(path.join(stage, 'dir'));
      fs.writeFileSync(path.join(stage, 'dir', 'file.txt'), 'legit-content');
      const archive = path.join(work, 'good.tar');
      await tar.c({ file: archive, cwd: stage, portable: true }, ['dir']);

      const target = targetDir();
      await extractArchive(archive, target);

      expect(fs.readFileSync(path.join(target, 'dir', 'file.txt'), 'utf8')).toBe('legit-content');
    });
  });

  describe('fails loudly on formats it cannot handle', () => {
    it('names bzip2 rather than failing obscurely', async () => {
      // BZh magic; the body does not need to be a valid stream to be classified.
      const archive = path.join(work, 'x.tar.bz2');
      fs.writeFileSync(archive, Buffer.concat([Buffer.from('BZh9'), Buffer.alloc(300)]));

      await expect(extractArchive(archive, targetDir())).rejects.toThrow(/bzip2/);
    });

    it('rejects a file that is not an archive at all', async () => {
      const archive = path.join(work, 'junk.bin');
      fs.writeFileSync(archive, Buffer.concat([Buffer.from([0, 1, 2]), Buffer.alloc(300)]));

      await expect(extractArchive(archive, targetDir())).rejects.toThrow(/Unable to detect archive format/);
    });
  });
});
