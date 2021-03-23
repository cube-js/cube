import chokidar from 'chokidar';
import { FSWatcher, createReadStream } from 'fs';
import path from 'path';

import { CubeCloudClient, AuthObject } from './cloud';
import { DeployDirectory } from './deploy';

export class LivePreviewWatcher {
  private watcher: FSWatcher | null = null;

  private handleQueueTimeout: NodeJS.Timeout | null = null;

  private cubeCloudClient = new CubeCloudClient();

  private auth: AuthObject | null = null;

  private queue: {}[] = [];

  public setAuth(token: string) {
    try {
      const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString());
      this.auth = {
        auth: token,
        deploymentId: payload.d,
        deploymentUrl: payload.dUrl,
        url: payload.url,
      };
    } catch (error) {
      console.error(error);
      throw new Error('Live-preview token is invalid');
    }
  }

  public startWatch(): void {
    if (!this.auth) throw new Error('Auth isn\'t set');
    if (!this.watcher) {
      const { deploymentUrl } = this.auth;
      console.log(`☁️  Start live-preview with Cube Cloud. Url: ${deploymentUrl}`);
      this.watcher = chokidar.watch(
        process.cwd(),
        {
          ignoreInitial: false,
          ignored: [
            '**/node_modules/**',
            '**/.*'
          ]
        }
      );

      let preSaveTimeout: NodeJS.Timeout;
      this.watcher.on('all', (/* event, p */) => {
        if (preSaveTimeout) clearTimeout(preSaveTimeout);

        preSaveTimeout = setTimeout(() => {
          this.queue.push({ time: new Date() });
        }, 1000);
      });

      this.handleQueue();
    }
  }

  public stopWatch(): void {
    if (this.watcher) {
      this.watcher.close();
      this.watcher = null;
    }

    if (this.handleQueueTimeout) clearTimeout(this.handleQueueTimeout);
    console.log('☁️  Stop live-preview');
  }

  public async getStatus() {
    const { auth } = this;
    if (!auth) throw new Error('Auth isn\'t set');
    const statusProps = await await this.cubeCloudClient.getStatusLivePreview({ auth });

    return {
      ...statusProps,
      enabled: !!this.watcher
    };
  }

  private async handleQueue() {
    try {
      const [job] = this.queue;
      if (job) {
        this.queue = [];
        await this.deploy();
      }
    } catch (error) {
      console.error(error);
    } finally {
      this.handleQueueTimeout = setTimeout(async () => this.handleQueue(), 1000);
    }
  }

  private async deploy(): Promise<Boolean> {
    if (!this.auth) throw new Error('Auth isn\'t set');
    console.log('Start upload files for live-preview');
    const { auth } = this;
    const directory = process.cwd();

    const deployDir = new DeployDirectory({ directory });
    const fileHashes: any = await deployDir.fileHashes();

    const upstreamHashes = await this.cubeCloudClient.getUpstreamHashes({ auth });
    const { transaction } = await this.cubeCloudClient.startUpload({ auth });

    const files = Object.keys(fileHashes);
    const fileHashesPosix: Record<string, any> = {};

    for (let i = 0; i < files.length; i++) {
      const file = files[i];

      const filePosix = file.split(path.sep).join(path.posix.sep);
      fileHashesPosix[filePosix] = fileHashes[file];

      if (!upstreamHashes[filePosix] || upstreamHashes[filePosix].hash !== fileHashes[file].hash) {
        await this.cubeCloudClient.uploadFile({
          auth,
          transaction,
          fileName: filePosix,
          data: createReadStream(path.join(directory, file))
        });
      }
    }
    await this.cubeCloudClient.finishUpload({ transaction, files: fileHashesPosix, auth });
    console.log('End upload files for live-preview');

    return true;
  }
}
