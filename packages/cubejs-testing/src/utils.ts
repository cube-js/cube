import { platform } from 'os';

export function getLocalHostnameByOs() {
  if (platform() === 'win32') {
    return 'docker.for.win.localhost';
  }

  if (platform() === 'darwin') {
    return 'host.docker.internal';
  }

  return 'localhost';
}
