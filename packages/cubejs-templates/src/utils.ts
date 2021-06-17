import fs from 'fs-extra';
import path from 'path';
import spawn from 'cross-spawn';
import fetch from 'node-fetch';
import { getHttpAgentForProxySettings } from '@cubejs-backend/shared';

export type File = {
  fileName: string;
  content: string;
};

export async function fileContentsRecursive(dir: string, rootPath?: string, includeNodeModules: boolean = false) {
  if (!rootPath) {
    rootPath = dir;
  }
  if (!fs.pathExistsSync(dir)) {
    return [];
  }
  if ((dir.includes('node_modules') && !includeNodeModules) || dir.includes('.git')) {
    return [];
  }

  const files = fs.readdirSync(dir);

  return (
    await Promise.all<File[]>(
      files.map(async (file) => {
        const fileName = path.join(dir, file);
        const stats = await fs.lstat(fileName);
        if (!stats.isDirectory()) {
          const content = fs.readFileSync(fileName, 'utf-8');

          return [
            {
              fileName: fileName.replace(<string>rootPath, '').replace(/\\/g, '/'),
              content,
            },
          ];
        } else {
          return fileContentsRecursive(fileName, rootPath, includeNodeModules);
        }
      })
    )
  ).reduce((a, b) => a.concat(b), []);
}

export async function proxyFetch(url) {
  return fetch(
    url,
    {
      agent: await getHttpAgentForProxySettings(),
    }
  );
}
