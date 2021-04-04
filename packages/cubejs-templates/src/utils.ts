import fs from 'fs-extra';
import path from 'path';
import spawn from 'cross-spawn';
import fetch from 'node-fetch';
import HttpsProxyAgent from 'http-proxy-agent';
import { exec } from 'child_process';

export type File = {
  fileName: string;
  content: string;
};

export async function fileContentsRecursive(dir: string, rootPath?: string, includeNodeModules: boolean = false) {
  if (!rootPath) {
    rootPath = dir;
  }
  if (!(await fs.pathExists(dir))) {
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

export async function executeCommand(command, args, options = {}) {
  const child = spawn(command, args, { stdio: 'inherit', ...options });

  return new Promise<void>((resolve, reject) => {
    child.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`${command} ${args.join(' ')} failed with exit code ${code}. Please check your console.`));
        return;
      }
      resolve();
    });
  });
}

function getCommandOutput(command) {
  return new Promise<string>((resolve, reject) => {
    exec(command, (error, stdout) => {
      if (error) {
        reject(error.message);
        return;
      }
      resolve(stdout);
    });
  });
}

export async function proxyFetch(url) {
  const [proxy] = (
    await Promise.all([getCommandOutput('npm config get https-proxy'), getCommandOutput('npm config get proxy')])
  )
    .map((s) => s.trim())
    .filter((s) => !['null', 'undefined', ''].includes(s));

  return fetch(
    url,
    proxy
      ? {
        // @ts-ignore
        agent: new HttpsProxyAgent(proxy),
      }
      : {}
  );
}
