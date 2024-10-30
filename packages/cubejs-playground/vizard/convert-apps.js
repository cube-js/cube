import fs from 'fs';
import path from 'path';
import { minimatch } from 'minimatch';

const baseDir = path.resolve('./apps');

function parseGitignore(dir) {
  const gitignorePath = path.join(dir, '.gitignore');
  let ignorePatterns = [];
  if (fs.existsSync(gitignorePath)) {
    const gitignoreContent = fs.readFileSync(gitignorePath, 'utf-8');
    ignorePatterns = gitignoreContent
      .split(/\r?\n/)
      .filter((line) => line.trim() !== '' && !line.startsWith('#'));
  }
  ignorePatterns.push('.gitignore'); // Always ignore the .gitignore file itself
  return ignorePatterns;
}

function shouldBeIgnored(filePath, baseDir, ignorePatterns) {
  const relativePath = path.relative(baseDir, filePath).replace(/\\/g, '/');
  return ignorePatterns.some((pattern) =>
    minimatch(relativePath, pattern, { dot: true, matchBase: false })
  );
}

function readDirRecursive(dir, baseDir, ignorePatterns, result = {}) {
  fs.readdirSync(dir).forEach((file) => {
    const filePath = path.join(dir, file);
    if (shouldBeIgnored(filePath, baseDir, ignorePatterns)) {
      console.log(`Ignoring ${filePath}`);
      return; // Skip ignored files and folders
    }
    const stat = fs.statSync(filePath);
    if (stat.isDirectory()) {
      result[file] = { directory: {} };
      readDirRecursive(
        filePath,
        baseDir,
        ignorePatterns,
        result[file].directory
      );
    } else if (stat.isFile()) {
      const contents = fs.readFileSync(filePath, 'utf-8');
      result[file] = { file: { contents } };
    }
  });
  return result;
}

fs.readdir(baseDir, (err, folders) => {
  if (err) {
    console.error('Error reading apps directory:', err);
    return;
  }
  const apps = {};

  folders.forEach((folder) => {
    const folderPath = path.join(baseDir, folder);
    if (fs.statSync(folderPath).isDirectory()) {
      // Check for .vizardignore file
      try {
        fs.accessSync(path.join(folderPath, '.vizardignore'));
        console.log(`Skipping ${folderPath} due to .vizardignore`);
        return;
      } catch (error) {
        // .vizardignore not found, proceed
      }

      const ignorePatterns = parseGitignore(folderPath);
      const result = {};
      readDirRecursive(folderPath, folderPath, ignorePatterns, result);

      apps[folder] = result;
    }
  });

  const outputPath = `./src/apps.json`;
  fs.writeFileSync(outputPath, JSON.stringify(apps, null, 2));
  console.log(`Output written to ${outputPath}`);
});
