import { spawnSync } from 'child_process';
import process from 'process';
import { internalExceptions } from './errors';
import { displayCLIWarning, displayCLIWarningOnce } from './cli';

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

type IsNativeSupportedResult = true | {
  reason: string
};

export function isNativeSupported(): IsNativeSupportedResult {
  if (process.platform === 'linux') {
    if (detectLibc() === 'musl') {
      displayCLIWarningOnce(
        'is-native-supported',
        'Unable to load native on your system. You are using linux distro with Musl which is not supported.'
      );

      return {
        reason: 'You are using linux distro with Musl which is not supported'
      };
    }
  }

  return true;
}

export enum LibraryExistsResult {
  // We are sure that required library doesnt exist on system
  None,
  // We are sure that required library exists
  Exists,
  UnableToCheck
}

export function libraryExists(libraryName: string): LibraryExistsResult {
  if (process.platform === 'linux') {
    try {
      const { status, output } = spawnSync('ldconfig', ['-v'], {
        encoding: 'utf8',
        // Using pipe to protect unexpect STDERR output
        stdio: 'pipe'
      });
      if (status === 0) {
        if (output.join(' ').includes(libraryName)) {
          return LibraryExistsResult.Exists;
        }

        return LibraryExistsResult.None;
      } else {
        return LibraryExistsResult.UnableToCheck;
      }
    } catch (e: any) {
      internalExceptions(e);
    }
  }

  return LibraryExistsResult.UnableToCheck;
}
