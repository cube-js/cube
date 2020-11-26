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

type RequireFromPackageOptions = {
  basePath?: string,
  relative: boolean,
  silent?: true
}

export type PackageManifest = {
  version: string,
  dependencies: Record<string, string>,
  devDependencies: Record<string, string>
}

export async function requireFromPackage<T = unknown>(
  pkg: string,
  { basePath = process.cwd(), relative, silent }: RequireFromPackageOptions
): Promise<T|null> {
  const exists = await packageExists(pkg, relative, basePath);
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

export async function resolvePackageVersion(basePath: string, pkgName: string) {
  const resolvedManifest = await requireFromPackage<PackageManifest|null>(
    path.join(pkgName, 'package.json'),
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
