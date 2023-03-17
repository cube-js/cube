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
  } catch (e: any) {
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
          `You are using ${process.env} platform on x86 which is not supported by Cube Store`,
        );
    }
  }

  if (process.arch === 'arm64') {
    switch (process.platform) {
      case 'linux':
        switch (detectLibc()) {
          case 'gnu':
            return 'aarch64-unknown-linux-gnu';
          default:
            throw new Error(
              `You are using ${process.env} platform on arm64 with MUSL as standard library which is not supported by Cube Store, please use libc (GNU)`,
            );
        }
      case 'darwin':
        // Rosetta 2 is required
        return 'x86_64-apple-darwin';
      default:
        throw new Error(
          `You are using ${process.env} platform on arm64 which is not supported by Cube Store`,
        );
    }
  }

  throw new Error(
    `You are using ${process.arch} architecture on ${process.platform} platform which is not supported by Cube Store`,
  );
}

export function isCubeStoreSupported(): boolean {
  if (process.arch === 'x64') {
    return ['win32', 'darwin', 'linux'].includes(process.platform);
  }

  if (process.arch === 'arm64') {
    // We mark darwin as supported, but it uses Rosetta 2
    if (process.platform === 'darwin') {
      return true;
    }

    return process.platform === 'linux' && detectLibc() === 'gnu';
  }

  return false;
}
