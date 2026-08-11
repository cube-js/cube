import { execFileSync } from 'child_process';
import fs from 'fs';
import os from 'os';
import path from 'path';

const DIST_ENTRY = path.join(__dirname, '..', 'dist', 'src', 'index.js');
const DIST_HTTP_UTILS = path.join(__dirname, '..', 'dist', 'src', 'http-utils.js');
const PROOF_SCRIPT = path.join(__dirname, 'fixtures', 'extract-archive.cjs');

function requireBuild(file: string): string {
  if (!fs.existsSync(file)) {
    throw new Error(`${file} is missing. Run \`yarn tsc\` before \`yarn unit\`.`);
  }

  return fs.readFileSync(file, 'utf-8');
}

describe('extractArchive', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cube-extract-archive-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  // `@xhmikosr/decompress` is ESM-only, so it can only be reached through a dynamic
  // import() that survives compilation. Guard the tsconfig setting that keeps it intact:
  // Node.js >= 22.12 can require() ESM, so a downleveled build would still pass the
  // runtime test below on CI's Node version while breaking on the supported Node 20.
  test('is compiled to a native import(), not a downleveled require()', () => {
    const compiled = requireBuild(DIST_HTTP_UTILS);

    expect(compiled).toContain("await import('@xhmikosr/decompress')");
    expect(compiled).not.toContain('require("@xhmikosr/decompress")');
  });

  test('extracts a .tar.gz archive when driven from CommonJS', () => {
    requireBuild(DIST_ENTRY);

    const srcDir = path.join(tmpDir, 'src');
    const outDir = path.join(tmpDir, 'out');
    const archivePath = path.join(tmpDir, 'archive.tar.gz');

    fs.mkdirSync(path.join(srcDir, 'nested'), { recursive: true });
    fs.writeFileSync(path.join(srcDir, 'manifest.json'), '{"name":"cube"}');
    fs.writeFileSync(path.join(srcDir, 'nested', 'deep.txt'), 'nested content');

    execFileSync('tar', ['-czf', archivePath, '-C', srcDir, '.']);

    // Runs in a real Node process — see the note in fixtures/extract-archive.cjs.
    execFileSync(process.execPath, [PROOF_SCRIPT, archivePath, outDir], { stdio: 'pipe' });

    expect(fs.readFileSync(path.join(outDir, 'manifest.json'), 'utf-8')).toBe('{"name":"cube"}');
    expect(fs.readFileSync(path.join(outDir, 'nested', 'deep.txt'), 'utf-8')).toBe('nested content');
  });
});
