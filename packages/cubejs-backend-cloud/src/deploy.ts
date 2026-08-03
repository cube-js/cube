import crypto from 'crypto';
import path from 'path';
import fs from 'fs-extra';
import { DotenvParseOutput } from '@cubejs-backend/dotenv';
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

type DeployHooks = {
  onStart?: (deploymentName: string, files: string[]) => void,
  onUpdate?: (i: number, { file }: { file: string}) => void,
  onUpload?: (files: string[], file: string) => void,
  onFinally?: () => void
};

export interface DeployResponse {
  lastHash?: string;
  [key: string]: any; // for other properties
}

export class DeployController {
  public constructor(
    protected readonly cubeCloudClient: CubeCloudClient,
    protected readonly envs: { envVariables?: DotenvParseOutput, replaceEnv?: boolean } = {},
    protected readonly hooks: DeployHooks = {}
  ) {
  }

  public async deploy(directory: string): Promise<DeployResponse> {
    let result;
    const deployDir = new DeployDirectory({ directory });
    const fileHashes: any = await deployDir.fileHashes();

    const upstreamHashes = await this.cubeCloudClient.getUpstreamHashes();
    const { transaction, deploymentName } = await this.cubeCloudClient.startUpload();

    if (this.envs.envVariables) {
      const { envVariables, replaceEnv } = this.envs;
      if (Object.keys(this.envs.envVariables).length) {
        await this.cubeCloudClient.setEnvVars({ envVariables, replaceEnv });
      }
    }

    const files = Object.keys(fileHashes);
    const fileHashesPosix: Record<string, any> = {};
    if (this.hooks.onStart) this.hooks.onStart(deploymentName, files);

    try {
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        if (this.hooks.onUpdate) this.hooks.onUpdate(i, { file });

        const filePosix = file.split(path.sep).join(path.posix.sep);
        fileHashesPosix[filePosix] = fileHashes[file];

        if (!upstreamHashes[filePosix] || upstreamHashes[filePosix].hash !== fileHashes[file].hash) {
          if (this.hooks.onUpload) this.hooks.onUpload(files, file);
          await this.cubeCloudClient.uploadFile({
            transaction,
            fileName: filePosix,
            data: fs.createReadStream(path.join(directory, file))
          });
        }
      }
      
      result = await this.cubeCloudClient.finishUpload({ transaction, files: fileHashesPosix });
    } finally {
      if (this.hooks.onFinally) this.hooks.onFinally();
    }

    return result || {};
  }
}
