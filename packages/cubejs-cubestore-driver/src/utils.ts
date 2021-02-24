import process from 'process';

export function isCubeStoreSupported(): boolean {
  if (process.arch !== 'x64') {
    return false;
  }

  return ['win32', 'darwin', 'linux'].includes(process.platform);
}
