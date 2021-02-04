import crypto from 'crypto';
import fs from 'fs-extra';
import path from 'path';

type DeployDirectoryOptions = {
  directory: string,
};

export class DeployDirectory {
  public constructor(
    protected readonly options: DeployDirectoryOptions
  ) { }

  public async fileHashes(directory: string = this.options.directory) {
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
        result[path.relative(this.options.directory, filePath)] = {
          hash: await this.fileHash(filePath)
        };
      }
    }
    return result;
  }

  protected filter(file: string) {
    const baseName = path.basename(file);

    // whitelist
    if (['.gitignore'].includes(baseName)) {
      return true;
    }

    // blacklist
    if (['dashboard-app', 'node_modules'].includes(baseName)) {
      return false;
    }

    return baseName.charAt(0) !== '.';
  }

  protected fileHash(file: string) {
    return new Promise((resolve, reject) => {
      const hash = crypto.createHash('sha1');
      const stream = fs.createReadStream(file);

      stream.on('error', err => reject(err));
      stream.on('data', chunk => hash.update(chunk));
      stream.on('end', () => resolve(hash.digest('hex')));
    });
  }
}
