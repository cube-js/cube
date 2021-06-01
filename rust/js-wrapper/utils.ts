import { spawnSync } from 'child_process';
import process from 'process';
import { displayCLIWarning, internalExceptions } from '@cubejs-backend/shared';

export function detectLibc() {
  if (process.platform !== 'linux') {
    throw new Error('Unable to detect libc on not linux os');
  }

  try {
    const { status } = spawnSync('getconf', ['GNU_LIBC_VERSION'], {
      encoding: 'utf8',
      // Using pipe to protect unexpect STDERR output
      stdio: 'pipe'
    });
    if (status === 0) {
      return 'gnu';
    }
  } catch (e) {
    internalExceptions(e);
  }

  {
    const { status, stdout, stderr } = spawnSync('ldd', ['--version'], {
      encoding: 'utf8',
      // Using pipe to protect unexpect STDERR output
      stdio: 'pipe',
    });
    if (status === 0) {
      if (stdout.includes('musl')) {
        return 'musl';
      }

      if (stdout.includes('gnu')) {
        return 'gnu';
      }
    } else {
      if (stderr.includes('musl')) {
        return 'musl';
      }

      if (stderr.includes('gnu')) {
        return 'gnu';
      }
    }
  }

  displayCLIWarning('Unable to detect what host library is used as libc, continue with gnu');

  return 'gnu';
}

export function getTarget(): string {
  if (process.arch === 'x64') {
    switch (process.platform) {
      case 'win32':
        return 'x86_64-pc-windows-msvc';
      case 'linux':
        return `x86_64-unknown-linux-${detectLibc()}`;
      case 'darwin':
        return 'x86_64-apple-darwin';
      default:
        throw new Error(
          `You are using ${process.env} platform which is not supported by Cube Store`,
        );
    }
  }

  if (process.arch === 'arm64' && process.platform === 'darwin') {
    // Rosetta 2 is required
    return 'x86_64-apple-darwin';
  }

  throw new Error(
    `You are using ${process.arch} architecture on ${process.platform} platform which is not supported by Cube Store`,
  );
}

export function isCubeStoreSupported(): boolean {
  if (process.arch === 'x64') {
    return ['win32', 'darwin', 'linux'].includes(process.platform);
  }

  return process.arch === 'arm64' && process.platform === 'darwin';
}
