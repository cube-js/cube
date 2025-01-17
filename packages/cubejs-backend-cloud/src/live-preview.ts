import chokidar from 'chokidar';

import { internalExceptions } from '@cubejs-backend/shared';

import { CubeCloudClient, AuthObject } from './cloud';
import { DeployController } from './deploy';

export class LivePreviewWatcher {
  private watcher: chokidar.FSWatcher | null = null;

  private handleQueueTimeout: NodeJS.Timeout | null = null;

  private cubeCloudClient = new CubeCloudClient();

  private auth: AuthObject | null = null;

  private queue: {}[] = [];

  private uploading: Boolean = false;

  private lastHash: string | undefined;

  private log(message: string) {
    console.log('☁️  Live-preview:', message);
  }

  public setAuth(token: string): AuthObject {
    try {
      const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString());
      this.auth = {
        auth: token,
        deploymentId: payload.deploymentId,
        url: payload.url,
      };

      return this.auth;
    } catch (e: any) {
      internalExceptions(e);
      throw new Error('Live-preview token is invalid');
    }
  }

  public startWatch(): void {
    if (!this.auth) {
      throw new Error('Auth isn\'t set');
    }

    if (!this.watcher) {
      this.log('Start with Cube Cloud');
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

  public stopWatch(message?: string): void {
    if (this.watcher) {
      this.watcher.close();
      this.watcher = null;
    }

    if (this.handleQueueTimeout) clearTimeout(this.handleQueueTimeout);
    this.log(`stop wathcer, ${message}`);
  }

  public async getStatus() {
    const { auth } = this;
    let result = {
      lastHashTarget: this.lastHash,
      uploading: this.uploading,
      active: Boolean(this.watcher),
      deploymentId: '' as any,
      url: '' as any,
    };

    if (auth) {
      result = {
        ...result,
        ...(await this.cubeCloudClient.getStatusDevMode({
          auth,
          lastHash: this.lastHash
        })),
        deploymentId: auth.deploymentId,
        url: auth.url
      };
    }

    return result;
  }

  public async createTokenWithPayload(payload: Record<string, any>) {
    let token;
    const { auth } = this;
    if (auth) {
      token = await this.cubeCloudClient.createTokenDevMode({ auth, payload });
    }

    return token;
  }

  private async handleQueue() {
    try {
      const [job] = this.queue;
      if (job) {
        this.queue = [];
        this.uploading = true;
        await this.deploy();
      }
    } catch (e: any) {
      if (e.response && e.response.statusCode === 302) {
        this.auth = null;
        this.stopWatch('token expired or invalid, please re-run live-preview mode');
      } else {
        internalExceptions(e);
      }
    } finally {
      this.uploading = false;
      this.handleQueueTimeout = setTimeout(async () => this.handleQueue(), 1000);
    }
  }

  private async deploy(): Promise<any> {
    if (!this.auth) throw new Error('Auth isn\'t set');
    this.log('files upload start');
    const { auth } = this;
    const directory = process.cwd();

    const cubeCloudClient = new CubeCloudClient(auth, true);
    const deployController = new DeployController(cubeCloudClient);

    const result = await deployController.deploy(directory);
    if (result && result.lastHash) this.lastHash = result.lastHash;

    this.log('files upload end, success');
    return result;
  }
}
