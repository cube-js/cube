import path from 'path';
import * as fs from 'fs';

export const packageExists = (
  moduleName: string,
  relative: boolean = false,
  basePath = process.cwd()
) => {
  if (relative) {
    try {
      // eslint-disable-next-line global-require,import/no-dynamic-require
      require.resolve(`${moduleName}`);

      return true;
    } catch (error) {
      return false;
    }
  }

  const modulePath = path.join(basePath, 'node_modules', moduleName);
  return fs.existsSync(modulePath);
};

export type PackageManifest = {
  version: string,
  dependencies: Record<string, string>,
  devDependencies: Record<string, string>
};

type RequireBaseOptions = {
  basePath?: string,
  relative?: boolean,
};

type RequireOptions = RequireBaseOptions & { silent?: true };

export function requireFromPackage<T = unknown | null>(
  pkg: string,
  opts: RequireBaseOptions & { silent: true }
): T | null;

export function requireFromPackage<T = unknown>(
  pkg: string,
  opts?: RequireBaseOptions
): T;

export function requireFromPackage<T = unknown|null>(pkg: string, options?: RequireOptions): T|null {
  const { basePath = process.cwd(), relative = false, silent = undefined } = options || {};

  const exists = packageExists(pkg, relative, basePath);
  if (!exists) {
    if (silent) {
      return null;
    }

    throw new Error(
      `${pkg} dependency not found. Please run this command from project directory.`
    );
  }

  if (relative) {
    const resolvePath = require.resolve(`${pkg}`);

    // eslint-disable-next-line global-require,import/no-dynamic-require
    return require(resolvePath);
  }

  // eslint-disable-next-line global-require,import/no-dynamic-require
  return require(path.join(basePath, 'node_modules', pkg));
}

export function isSslKey(content: string) {
  return content.startsWith('-----BEGIN RSA PRIVATE KEY-----');
}

export function isSslCert(content: string) {
  return content.startsWith('-----BEGIN CERTIFICATE-----');
}

export function isFilePath(fp: string): boolean {
  if (fp === '') {
    return false;
  }

  const resolvedPath = path.parse(fp);
  if ((resolvedPath.root || resolvedPath.dir) && resolvedPath.name) {
    return true;
  }

  return false;
}

export function requirePackageManifest<T = PackageManifest>(
  pkg: string,
  opts: RequireBaseOptions & { silent: true }
): Promise<T|null>;
export function requirePackageManifest<T = PackageManifest>(
  pkg: string,
  opts?: RequireBaseOptions
): Promise<T>;
export async function requirePackageManifest(
  pkgName: string,
  options?: RequireOptions
) {
  return requireFromPackage<PackageManifest>(
    path.join(pkgName, 'package.json'),
    options
  );
}

export async function resolvePackageVersion(basePath: string, pkgName: string) {
  const resolvedManifest = await requirePackageManifest(
    pkgName,
    {
      basePath,
      relative: false,
      silent: true,
    },
  );
  if (resolvedManifest) {
    return resolvedManifest.version;
  }

  return null;
}

export async function resolveBuiltInPackageVersion(pkgName: string) {
  return resolvePackageVersion(
    '/cube',
    pkgName,
  );
}

export async function resolveUserPackageVersion(pkgName: string) {
  return resolvePackageVersion(
    // In the official docker image, it will be resolved to /cube/conf
    process.cwd(),
    pkgName,
  );
}
