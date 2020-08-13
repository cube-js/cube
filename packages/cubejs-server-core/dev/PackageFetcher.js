const fs = require('fs-extra');
const fetch = require('node-fetch').default;
const decompress = require('decompress');
const decompressTargz = require('decompress-targz');
const path = require('path');

const { executeCommand } = require('./utils');

class PackageFetcher {
  constructor(repo) {
    this.repo = repo;
    this.tmpFolderPath = path.resolve('.', 'node_modules', '.tmp');

    try {
      fs.mkdirSync(this.tmpFolderPath);
    } catch (err) {
      if (err.code === 'EEXIST') {
        fs.removeSync(this.tmpFolderPath);
        fs.mkdirSync(this.tmpFolderPath);
      } else {
        throw err;
      }
    }

    this.repoArchivePath = `${this.tmpFolderPath}/master.tar.gz`;
  }

  async manifestJSON() {
    const response = await fetch(
      `https://api.github.com/repos/${this.repo.owner}/${this.repo.name}/contents/manifest.json`
    );

    return JSON.parse(Buffer.from((await response.json()).content, 'base64').toString());
  }

  async downloadRepo() {
    const url = `https://github.com/${this.repo.owner}/${this.repo.name}/archive/master.tar.gz`;
    const writer = fs.createWriteStream(this.repoArchivePath);

    (await fetch(url)).body.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on('finish', resolve);
      writer.on('error', reject);
    });
  }

  async downloadPackages() {
    await this.downloadRepo();

    await decompress(this.repoArchivePath, this.tmpFolderPath, {
      plugins: [decompressTargz()],
    });

    const dir = fs.readdirSync(this.tmpFolderPath).find((name) => !name.endsWith('tar.gz'));
    await executeCommand('npm', ['install'], { cwd: path.resolve(this.tmpFolderPath, dir) });

    return {
      packagesPath: path.join(this.tmpFolderPath, dir, 'packages'),
    };
  }

  cleanup() {
    fs.removeSync(this.tmpFolderPath);
  }
}

module.exports = PackageFetcher;
