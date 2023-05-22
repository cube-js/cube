import { spawnSync } from 'child_process';
import { internalExceptions } from './errors';
import { displayCLIWarning } from './cli';

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
