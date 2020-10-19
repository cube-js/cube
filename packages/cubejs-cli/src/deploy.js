const path = require('path');
const fs = require('fs-extra');
const crypto = require('crypto');

class Deploy {
  constructor({ directory }) {
    this.directory = directory;
  }

  filter(file) {
    const baseName = path.basename(file);
    return baseName !== 'node_modules' && baseName !== '.git' && baseName !== '.env';
  }

  async fileHashes(directory) {
    directory = directory || this.directory;
    let result = {};

    const files = await fs.readdir(directory);
    // eslint-disable-next-line no-restricted-syntax
    for (const file of files) {
      const filePath = path.resolve(directory, file);
      if (!this.filter(filePath)) {
        // eslint-disable-next-line no-continue
        continue;
      }
      const stat = await fs.stat(filePath);
      if (stat.isDirectory()) {
        result = { ...result, ...await this.fileHashes(filePath) };
      } else {
        result[path.relative(this.directory, filePath)] = {
          hash: await this.fileHash(filePath)
        };
      }
    }
    return result;
  }

  fileHash(file) {
    return new Promise((resolve, reject) => {
      const hash = crypto.createHash('sha1');
      const stream = fs.createReadStream(file);
      stream.on('error', err => reject(err));
      stream.on('data', chunk => hash.update(chunk));
      stream.on('end', () => resolve(hash.digest('hex')));
    });
  }
}

module.exports = Deploy;
