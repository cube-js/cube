/**
 * This file is based on  https://github.com/automation-stack/node-machine-id
 * @author Copyright (c) 2016 Aleksandr Komlev, licensed under MIT
 */
import { exec, execSync } from 'child_process';
import { createHash } from 'crypto';

function isWindowsProcessMixedOrNativeArchitecture(): 'native' | 'mixed' | '' {
  // detect if the node binary is the same arch as the Windows OS.
  // or if this is 32 bit node on 64 bit windows.
  if (process.platform !== 'win32') {
    return '';
  }

  if (process.arch === 'ia32' && process.env.hasOwnProperty('PROCESSOR_ARCHITEW6432')) {
    return 'mixed';
  }

  return 'native';
}

const win32RegBinPath: Record<string, string> = {
  '': '',
  native: '%windir%\\System32',
  mixed: '%windir%\\sysnative\\cmd.exe /c %windir%\\System32'
};

const guid: Record<string, string> = {
  darwin: 'ioreg -rd1 -c IOPlatformExpertDevice',
  win32: `${win32RegBinPath[isWindowsProcessMixedOrNativeArchitecture()]}\\REG.exe ` +
    'QUERY HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography ' +
    '/v MachineGuid',
  linux: '( cat /var/lib/dbus/machine-id /etc/machine-id 2> /dev/null || hostname ) | head -n 1 || :',
  freebsd: 'kenv -q smbios.system.uuid || sysctl -n kern.hostuuid'
};

function hash(input: string): string {
  return createHash('sha256').update(input).digest('hex');
}

function expose(platform: NodeJS.Platform, result: string): string {
  switch (platform) {
    case 'darwin':
      return result
        .split('IOPlatformUUID')[1]
        .split('\n')[0].replace(/=|\s+|"/ig, '')
        .toLowerCase();
    case 'win32':
      return result
        .toString()
        .split('REG_SZ')[1]
        .replace(/\r+|\n+|\s+/ig, '')
        .toLowerCase();
    case 'linux':
      return result
        .toString()
        .replace(/\r+|\n+|\s+/ig, '')
        .toLowerCase();
    case 'freebsd':
      return result
        .toString()
        .replace(/\r+|\n+|\s+/ig, '')
        .toLowerCase();
    default:
      throw new Error(`Unsupported platform: ${process.platform}`);
  }
}

export function machineIdSync(original: boolean = false): string {
  if (process.platform in guid) {
    const id: string = expose(
      process.platform,
      execSync(
        guid[process.platform],
        // Using pipe to protect unexpect STDERR output
        { stdio: 'pipe' }
      ).toString()
    );
    return original ? id : hash(id);
  }

  throw new Error(`Unsupported platform: ${process.platform}`);
}

export function machineId(original: boolean = false): Promise<string> {
  return new Promise((resolve: Function, reject: Function): Object => {
    if (!guid[process.platform]) {
      return reject(new Error(`Unsupported platform: ${process.platform}`));
    }

    return exec(guid[process.platform], {}, (err, stdout) => {
      // This is executing in a callback, so any Exceptions thrown will
      // not reject the promise
      try {
        if (err) {
          return reject(
            new Error(`Error while obtaining machine id: ${err.stack}`)
          );
        }

        const id: string = expose(process.platform, stdout.toString());
        return resolve(original ? id : hash(id));
      } catch (exception) {
        return reject(exception);
      }
    });
  });
}
