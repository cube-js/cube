const fs = require('fs-extra');
const path = require('path');
const spawn = require('cross-spawn');

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
          const content = await fs.readFile(fileName, 'utf-8');
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

function executeCommand(command, args, options = {}) {
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

module.exports = {
  fileContentsRecursive,
  executeCommand,
};
