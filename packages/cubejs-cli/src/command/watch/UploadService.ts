import cliProgress, { SingleBar } from 'cli-progress';
import fs from 'fs-extra';
import path from 'path';
import { Config } from '../../config';
import { DeployDirectory } from '../../deploy';

type Options = {
  isWatchMode?: boolean;
  isLivePreview?: boolean;
  uploadEnv?: boolean;
  onUploadStart?: (deploymentName: string, files: string[]) => void;
  onUploadDone?: () => void;
};

export class UploadService {
  private upstreamHashes: any[] = [];

  private deployDirectory: DeployDirectory;

  public bar: SingleBar;

  public constructor(
    private readonly config: Config,
    private readonly directory: string,
    private readonly options: Options = {}
  ) {
    this.deployDirectory = new DeployDirectory(directory);

    this.bar = new cliProgress.SingleBar({
      format: '- Uploading files | {bar} | {percentage}% || {value} / {total} | {file}',
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
      hideCursor: true,
      clearOnComplete: true,
    });
  }

  public async upload() {
    await this.fetchUpstreamHashes();
    const fileHashes = await this.deployDirectory.fileHashes();

    const qs = {
      watch: this.options.isWatchMode,
      live: this.options.isLivePreview,
    };

    const { transaction, deploymentName } = await this.config.cloudReq({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/start-upload`,
      method: 'POST',
      qs,
    });

    if (this.options.uploadEnv) {
      await this.uploadEnv();
    }

    const files = Object.keys(fileHashes);
    const fileHashesPosix = {};

    this.options.onUploadStart?.(deploymentName, files);

    this.bar.start(files.length, 0, {
      file: '',
    });

    try {
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        this.bar.update(i, { file });

        const filePosix = file.split(path.sep).join(path.posix.sep);
        fileHashesPosix[filePosix] = fileHashes[file];

        if (!this.upstreamHashes[filePosix] || this.upstreamHashes[filePosix].hash !== fileHashes[file].hash) {
          await this.config.cloudReq({
            url: (deploymentId: string) => `build/deploy/${deploymentId}/upload-file`,
            method: 'POST',
            formData: {
              transaction: JSON.stringify(transaction),
              fileName: filePosix,
              file: {
                value: fs.createReadStream(path.join(this.directory, file)),
                options: {
                  filename: path.basename(file),
                  contentType: 'application/octet-stream',
                },
              },
            },
            qs,
          });
        }
      }

      this.bar.update(files.length, { file: 'Post processing...' });

      await this.config.cloudReq({
        url: (deploymentId: string) => `build/deploy/${deploymentId}/finish-upload`,
        method: 'POST',
        body: {
          transaction,
          files: fileHashesPosix,
        },
        qs,
      });
    } finally {
      this.bar.stop();
    }

    this.options.onUploadDone?.();
  }

  public async fetchUpstreamHashes() {
    this.upstreamHashes = await this.config.cloudReq({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/files`,
      method: 'GET',
      qs: {
        watch: this.options.isWatchMode,
      },
    });
  }

  public async uploadEnv() {
    const envVariables = await this.config.envFile(`${this.directory}/.env`);
    await this.config.cloudReq({
      url: (deploymentId) => `build/deploy/${deploymentId}/set-env`,
      method: 'POST',
      body: {
        envVariables: JSON.stringify(envVariables),
      },
      qs: {
        watch: this.options.isWatchMode,
      },
    });
  }
}
