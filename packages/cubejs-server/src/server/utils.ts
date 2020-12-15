import { SemVer } from 'semver';
import * as lockfile from '@yarnpkg/lockfile';
import * as fs from 'fs';
import * as path from 'path';
import { internalExceptions } from '@cubejs-backend/shared';

const devPackages = [
  'typescript',
];

export function isCubeNotServerPackage(pkgName: string): boolean {
  return pkgName !== '@cubejs-backend/server' && pkgName.toLowerCase().startsWith('@cubejs-backend/');
}

export function isCubePackage(pkgName: string): boolean {
  return pkgName.toLowerCase().startsWith('@cubejs-backend/');
}

export function isDevPackage(pkgName: string): boolean {
  return isCubePackage(pkgName) || devPackages.includes(pkgName.toLowerCase());
}

export function isSimilarPackageRelease(pkg: SemVer, core: SemVer): boolean {
  if (pkg.major === 0 && core.major === 0) {
    return pkg.minor === core.minor;
  }

  return pkg.major === core.major;
}

export function getMajorityVersion(pkg: SemVer, strict: boolean = false): string {
  if (pkg.major === 0) {
    if (strict) {
      return `^${pkg.major}.${pkg.minor}.${pkg.patch}`;
    }

    return `^${pkg.major}.${pkg.minor}`;
  }

  if (strict) {
    return `^${pkg.major}.${pkg.minor}`;
  }

  return `^${pkg.major}`;
}

export type ProjectLock = {
  resolveVersion: (pkg: string) => string|null
}

export function parseNpmLock(): ProjectLock|null {
  const file = fs.readFileSync(
    path.join(process.cwd(), 'package-lock.json'),
    'utf8'
  );

  try {
    const lock = JSON.parse(file);

    if (!lock) {
      return null;
    }

    if (!lock.dependencies) {
      return null;
    }

    return {
      resolveVersion: (pkg: string) => {
        if (pkg in lock.dependencies) {
          return lock.dependencies[pkg].version;
        }

        return null;
      },
    };
  } catch (e) {
    internalExceptions(e);

    return null;
  }
}

export function parseYarnLock(): ProjectLock|null {
  const file = fs.readFileSync(
    path.join(process.cwd(), 'yarn.lock'),
    'utf8'
  );

  const { type, object } = lockfile.parse(file);

  if (type === 'success') {
    return {
      resolveVersion: (pkg: string) => {
        if (pkg in object) {
          return object[pkg].version;
        }

        return null;
      },
    };
  }

  return null;
}
