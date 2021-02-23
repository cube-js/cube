import colors from '@oclif/color';
import { spawnSync } from 'child_process';

export const displayWarning = (message: string) => {
  console.log(`${colors.yellow('Warning.')} ${message}`);
};

export function detectLibc() {
  if (process.platform !== 'linux') {
    throw new Error('Unable to detect libc on not linux os');
  }

  {
    const { status } = spawnSync('ldd', ['--version'], {
      encoding: 'utf8',
    });
    if (status === 0) {
      return 'gnu';
    }
  }

  {
    const { status, stdout, stderr } = spawnSync('ldd', ['--version'], {
      encoding: 'utf8',
    });
    if (status === 0) {
      if (stdout.includes('musl')) {
        return 'musl';
      }

      if (stdout.includes('musl')) {
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

  displayWarning('Unable to detect what host library is used as libc, continue with gnu');

  return 'gnu';
}
