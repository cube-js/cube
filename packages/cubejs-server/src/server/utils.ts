import { SemVer } from 'semver';

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
