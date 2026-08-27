import fs from 'fs';
import os from 'os';
import path from 'path';
import * as tar from 'tar';
import { crc32 } from 'zlib';

import { extractArchive } from '../src/http-utils';

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

    it('does not write through a zip symlink that points outside the target', async () => {
      // The zip backend's containment is the half worth proving separately: a symlink
      // entry has a clean relative *name*, so only a check on the resolved destination
      // catches the entry written through it afterwards.
      const archive = path.join(work, 'zipsym.zip');
      const outside = path.join(work, 'outside');
      fs.mkdirSync(outside);

      await writeZip(archive, [
        { name: 'esc', content: outside, mode: 0o120777 },
        { name: 'esc/PWNED.txt', content: 'pwned-through-symlink' },
      ]);

      await expect(extractArchive(archive, targetDir())).rejects.toThrow(/out of bound path/i);
      expect(fs.existsSync(path.join(outside, 'PWNED.txt'))).toBe(false);
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
