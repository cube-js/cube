// https://github.com/cube-js/cube/issues/11215
// The published bundle (fesm2022/*.mjs) is plain ESM, and ng-packagr keeps bare
// import specifiers as written in src. Node's ESM resolver (used by vitest,
// Angular SSR, etc.) rejects directory imports such as 'fast-deep-equal/es6',
// so every bare specifier in src must resolve under Node ESM rules to a file.
// (import.meta.resolve does not itself reject directories, the loader does,
// hence the explicit isFile() check below.)
import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

const packageRoot = path.resolve(__dirname, '..');

function listSourceFiles(dir: string): string[] {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      return listSourceFiles(full);
    }
    return entry.name.endsWith('.ts') && !entry.name.endsWith('.spec.ts') ? [full] : [];
  });
}

function bareImportSpecifiers(): string[] {
  const specifiers = new Set<string>();
  const re = /(?:import|export)\s[^'"]*?from\s+['"]([^'"]+)['"]|import\s+['"]([^'"]+)['"]/g;
  for (const file of listSourceFiles(path.join(packageRoot, 'src'))) {
    const source = fs.readFileSync(file, 'utf8');
    for (const match of source.matchAll(re)) {
      const specifier = match[1] ?? match[2];
      if (!specifier.startsWith('.')) {
        specifiers.add(specifier);
      }
    }
  }
  return [...specifiers].sort();
}

function resolveAsEsm(specifier: string): string {
  try {
    return execFileSync(
      process.execPath,
      ['--input-type=module', '-e', 'process.stdout.write(import.meta.resolve(process.argv[1]))', specifier],
      { cwd: packageRoot, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
    );
  } catch (e: any) {
    const stderr = String(e.stderr || e.message);
    const line = stderr.split('\n').find((l) => l.includes('Error')) || stderr;
    throw new Error(`'${specifier}' does not resolve as ESM: ${line.trim()}`);
  }
}

describe('Issue #11215: bare imports resolve under Node ESM', () => {
  const specifiers = bareImportSpecifiers();

  it('finds bare imports in src', () => {
    expect(specifiers).toContain('@angular/core');
  });

  it.each(specifiers)('%s', (specifier) => {
    const resolved = resolveAsEsm(specifier);
    expect(resolved).toMatch(/^file:\/\//);
    const isFile = fs.statSync(fileURLToPath(resolved)).isFile();
    if (!isFile) {
      throw new Error(`'${specifier}' resolves to a directory (${resolved}); Node ESM rejects directory imports`);
    }
  });
});
