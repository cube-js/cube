import process from 'process';
import { displayCLIWarning, internalExceptions, detectLibc } from '@cubejs-backend/shared';

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
