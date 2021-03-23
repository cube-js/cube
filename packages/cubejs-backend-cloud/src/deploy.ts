import crypto from 'crypto';
import path from 'path';
import fs from 'fs-extra';
import cliProgress from 'cli-progress';
import { CubeCloudClient } from './cloud';

type DeployDirectoryOptions = {
  directory: string,
};

export class DeployDirectory {
  public constructor(
    protected readonly options: DeployDirectoryOptions
  ) { }

  public async fileHashes(directory: string = this.options.directory) {
    let result: Record<string, any> = {};

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

export class DeployController {
  public constructor(
    protected readonly cubeCloudClient: CubeCloudClient
  ) {
  }

  public async deploy(directory: string) {
    const bar = new cliProgress.SingleBar({
      format: '- Uploading files | {bar} | {percentage}% || {value} / {total} | {file}',
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
      hideCursor: true
    });

    console.log('Start upload files for live-preview');
    const deployDir = new DeployDirectory({ directory });
    const fileHashes: any = await deployDir.fileHashes();

    const upstreamHashes = await this.cubeCloudClient.getUpstreamHashes();
    const { transaction } = await this.cubeCloudClient.startUpload();

    const files = Object.keys(fileHashes);
    const fileHashesPosix: Record<string, any> = {};
    bar.start(files.length, 0, {
      file: ''
    });

    try {
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        bar.update(i, { file });

        const filePosix = file.split(path.sep).join(path.posix.sep);
        fileHashesPosix[filePosix] = fileHashes[file];

        if (!upstreamHashes[filePosix] || upstreamHashes[filePosix].hash !== fileHashes[file].hash) {
          bar.update(files.length, { file: 'Post processing...' });
          await this.cubeCloudClient.uploadFile({
            transaction,
            fileName: filePosix,
            data: fs.createReadStream(path.join(directory, file))
          });
        }
      }
      await this.cubeCloudClient.finishUpload({ transaction, files: fileHashesPosix });
    } finally {
      bar.stop();
    }

    return true;
  }
}
