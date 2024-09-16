import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';
import archiver from 'archiver';
import { execSync } from 'child_process';
import { fileURLToPath } from 'url';

import { APP_OPTIONS } from './src/app-options.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const appsDir = path.join(__dirname, 'apps');
const publicPreviewDir = path.join(__dirname, 'public', 'preview');
const publicDownloadDir = path.join(__dirname, 'public', 'download');
const srcDir = path.join(__dirname, 'src');
const statsJsonPath = path.join(srcDir, 'stats.json');

function validateOptions({
  visualizations,
  framework,
  language,
  library,
  appName,
}) {
  if (!APP_OPTIONS.framework.includes(framework)) {
    throw new Error(
      `Invalid framework: ${framework}. App name ${appName}. Available options are: ${JSON.stringify(APP_OPTIONS.framework)}`
    );
  }

  if (!APP_OPTIONS.language.includes(language)) {
    throw new Error(
      `Invalid language: ${language}. App name ${appName}. Available options are: ${JSON.stringify(APP_OPTIONS.language)}`
    );
  }

  if (!APP_OPTIONS.library.includes(library)) {
    throw new Error(
      `Invalid library: ${library}. App name ${appName}. Available options are: ${JSON.stringify(APP_OPTIONS.library)}`
    );
  }

  visualizations.forEach((visualization) => {
    if (!APP_OPTIONS.visualization.includes(visualization)) {
      throw new Error(
        `Invalid visualization: ${visualization}. App name ${appName}. Available options are: ${JSON.stringify(APP_OPTIONS.visualization)}`
      );
    }
  });

  return null;
}

/**
 * Creates an archive of a specified directory.
 *
 * @param {string} sourceDir Path to the directory you want to archive.
 * @param {string} outputFilePath Path where the output archive will be saved.
 */
async function createArchive(sourceDir, outputFilePath) {
  return new Promise((resolve, reject) => {
    // Ensure the source directory exists
    if (!fsSync.existsSync(sourceDir)) {
      reject(new Error(`Source directory "${sourceDir}" does not exist.`));
      return;
    }

    // Create a file to stream archive data to.
    const output = fsSync.createWriteStream(outputFilePath);
    const archive = archiver('zip', {
      zlib: { level: 9 }, // Sets the compression level.
    });

    // Listen for all archive data to be written
    output.on('close', function () {
      console.log(`${archive.pointer()} total bytes`);
      console.log(
        'Archiver has been finalized and the output file descriptor has closed.'
      );
      resolve();
    });

    // Good practice to catch this error explicitly
    archive.on('error', function (err) {
      reject(err);
    });

    // Pipe archive data to the file
    archive.pipe(output);

    // Append files from the source directory
    archive.glob('**/*', {
      cwd: sourceDir,
      ignore: ['node_modules/**', 'dist/**', 'build/**'],
    });

    // Finalize the archive (i.e., finish appending files and finalize the archive)
    archive.finalize();
  });
}

async function main() {
  try {
    // Ensure the public/preview, public/download and src directories exist
    await fs.mkdir(publicDownloadDir, { recursive: true });
    await fs.mkdir(publicPreviewDir, { recursive: true });
    await fs.mkdir(srcDir, { recursive: true });

    const stats = {};
    const dirents = await fs.readdir(appsDir, { withFileTypes: true });

    for (const dirent of dirents.filter((dirent) => dirent.isDirectory())) {
      const appName = dirent.name;
      const appDir = path.join(appsDir, appName);

      // Check for .vizardignore file
      try {
        await fs.access(path.join(appDir, '.vizardignore'));
        console.log(`Skipping ${appName} due to .vizardignore`);
        continue; // Skip this directory
      } catch (error) {
        // .vizardignore not found, proceed
      }

      const archiveFile = path.join(publicDownloadDir, `${appName}.zip`);

      await createArchive(appDir, archiveFile);

      console.log(`App archive created: ${appName}.zip`);

      console.log(`Processing ${appName}...`);

      // Run npm install and npm run build
      execSync('yarn install', { cwd: appDir, stdio: 'inherit' });
      execSync('yarn run build', { cwd: appDir, stdio: 'inherit' });

      // Move dist content
      const distDir = path.join(appDir, 'dist');
      const targetDir = path.join(publicPreviewDir, appName);
      await fs.mkdir(targetDir, { recursive: true });

      const files = await fs.readdir(distDir);
      for (const file of files) {
        const exist = await fs
          .stat(path.join(targetDir, file))
          .catch(() => null);
        if (exist) {
          await fs.rm(path.join(targetDir, file), { recursive: true });
        }
        await fs.rename(path.join(distDir, file), path.join(targetDir, file));
      }

      // Parse appName and update stats
      const [framework, language, library, visualizationList] =
        appName.split('-');
      const visualizations = visualizationList.split('+');

      try {
        validateOptions({
          visualizations,
          framework,
          language,
          library,
          appName,
        });

        visualizations.forEach((visualization) => {
          stats[visualization] = stats[visualization] || {};
          stats[visualization][framework] =
            stats[visualization][framework] || {};
          stats[visualization][framework][language] =
            stats[visualization][framework][language] || {};
          stats[visualization][framework][language][library] = appName;
        });
      } catch (e) {
        console.error(e);
      }
    }

    // Write stats.json
    await fs.writeFile(statsJsonPath, JSON.stringify(stats, null, 2));
    console.log(`stats.json has been generated at ${statsJsonPath}`);
  } catch (error) {
    console.error('An error occurred:', error);
  }
}

main();
