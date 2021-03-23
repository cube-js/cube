const fs = require('fs-extra');
const path = require('path');
const spawn = require('cross-spawn');
const fetch = require('node-fetch');
const HttpsProxyAgent = require('http-proxy-agent');
const { exec } = require('child_process');

async function fileContentsRecursive(dir, rootPath, includeNodeModules) {
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
    await Promise.all(
      files.map(async (file) => {
        const fileName = path.join(dir, file);
        const stats = await fs.lstat(fileName);
        if (!stats.isDirectory()) {
          const content = fs.readFileSync(fileName, 'utf-8');

          return [
            {
              fileName: fileName.replace(rootPath, '').replace(/\\/g, '/'),
              content,
            },
          ];
        } else {
          return fileContentsRecursive(
            fileName,
            rootPath,
            includeNodeModules
          );
        }
      })
    )
  ).reduce((a, b) => a.concat(b), []);
}

async function executeCommand(command, args, options = {}) {
  const child = spawn(command, args, { stdio: 'inherit', ...options });

  return new Promise((resolve, reject) => {
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
  return new Promise((resolve, reject) => {
    exec(command, (error, stdout) => {
      if (error) {
        reject(error.message);
        return;
      }
      resolve(stdout);
    });
  });
}

async function proxyFetch(url) {
  const [proxy] = (await Promise.all([
    getCommandOutput('npm config get https-proxy'),
    getCommandOutput('npm config get proxy'),
  ]))
    .map((s) => s.trim())
    .filter((s) => !['null', 'undefined', ''].includes(s));

  return fetch(
    url,
    proxy
      ? {
        agent: new HttpsProxyAgent(proxy),
      }
      : {}
  );
}

module.exports = {
  fileContentsRecursive,
  executeCommand,
  proxyFetch
};
