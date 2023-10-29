import crypto from 'crypto';
import fs from 'fs-extra';
import path from 'path';
import { ContentHash } from './command/watch/types';

type FileHash = {
  hash: string;
};

export class DeployDirectory {
  public constructor(protected readonly directory: string) {}

  public async fileHashes(directory: string = this.directory) {
    let result: Record<string, FileHash> = {};

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
        result = { ...result, ...(await this.fileHashes(filePath)) };
      } else {
        result[path.relative(this.directory, filePath)] = {
          hash: await this.fileHash(filePath),
        };
      }
    }
    return result;
  }

  public async contentHash(): Promise<ContentHash> {
    const pathsHash = crypto.createHash('sha1');
    const contentHash = crypto.createHash('sha1');

    Object.entries(await this.fileHashes())
      .sort(([a], [b]) => (a > b ? 1 : -1))
      .forEach(([filePath, file]) => {
        pathsHash.update(filePath);
        contentHash.update(file.hash);
      });

    return {
      pathsHash: pathsHash.digest('hex'),
      contentHash: contentHash.digest('hex'),
    };
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
    return new Promise<string>((resolve, reject) => {
      const hash = crypto.createHash('sha1');
      const stream = fs.createReadStream(file);

      stream.on('error', (err) => reject(err));
      stream.on('data', (chunk) => hash.update(chunk));
      stream.on('end', () => resolve(hash.digest('hex')));
    });
  }
}
