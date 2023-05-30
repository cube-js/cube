import 'source-map-support/register';

import {
  detectLibc,
  displayCLIError,
  displayCLIWarning, downloadAndExtractFile,
  libraryExists,
  LibraryExistsResult,
} from '@cubejs-backend/shared';
import * as process from 'process';

const pkg = require('../../package.json');

interface UrlVariable {
  resolve(url: string): string
}

function resolveConstraint(name: string, constraintDetails: any): boolean {
  if (name === 'platform') {
    return constraintDetails.includes(process.platform);
  }

  displayCLIWarning(`Unknown constraint name: ${name}, pass: false`);

  return false;
}

function resolveVariableValue(value: any): string | false {
  if (Array.isArray(value) && value.length == 2) {
    const [ valueName, supportedVersions ] = value;
    if (valueName === 'libpython') {
      for (const version of supportedVersions) {
        if (libraryExists(`libpython${version}`) === LibraryExistsResult.Exists) {
          return version;
        }
      }

      return false;
    }
  }

  displayCLIWarning(`Unable to resolve value, unknown value ${value}`);

  return false;
}

function resolveVars(variables: Record<string, any>): UrlVariable[] {
  const res = [];

  for (const [variableName, variable] of Object.entries(variables)) {
    let constraintPass = true;

    if (variable.constraints) {
        for (const [constraintName, constraintDetails] of Object.entries(variable.constraints)) {
          if (!resolveConstraint(constraintName, constraintDetails)) {
            constraintPass = false;
            break;
          }
        }
    }

    let value = variable['default'];

    if (constraintPass) {
      if (variable.value) {
        const resolvedValue = resolveVariableValue(variable.value);
        if (resolvedValue) {
          value = resolvedValue;
        }
      }
    }

    res.push({
      resolve(url: string): string {
        url = url.replace('${' + variableName + '}', value);

        return url;
      }
    })
  }

  return res;
}

function resolvePath(path: string, variables: UrlVariable[]): string {
  path = path.replace('${version}', pkg.version);
  path = path.replace('${platform}', process.platform);
  path = path.replace('${arch}', 'x64');

  if (process.platform === 'linux') {
    path = path.replace('${libc}', detectLibc());
  } else {
    path = path.replace('${libc}', 'unknown');
  }

  for (const variable of variables) {
    path = variable.resolve(path);
  }

  return path;
}

(async () => {
  try {
    if (!pkg.resources) {
      throw new Error('Please defined resources section in package.json file in corresponding package');
    }

    const variables = resolveVars(pkg.resources.vars);

    for (const file of pkg.resources.files) {
      const url = resolvePath(file.host + file.path, variables);
      console.log(`Downloading: ${url}`)

      await downloadAndExtractFile(
        url,
        {
          cwd: process.cwd(),
          showProgress: true
        }
      );
    }
  } catch (e: any) {
    await displayCLIError(e, 'Native Installer');
  }
})();
