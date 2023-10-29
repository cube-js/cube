import chokidar, { FSWatcher } from 'chokidar';
import { Config } from '../../config';
import { DeployDirectory } from '../../deploy';
import { debounce } from '../../utils';
import { DevModeStatus } from './types';
import { UploadService } from './UploadService';

type Options = {
  onDevModeStarted?: () => void;
  onBranchStatusChanged?: (status: DevModeStatus) => void;
  onWillUpload?: () => void;
  onDidUpload?: () => void;
};

export class WatchService {
  public readonly config: Config = new Config();

  private readonly uploader: UploadService;

  private readonly watcher: FSWatcher;

  private readonly deployDirectory: DeployDirectory;

  public constructor(public readonly directory: string, private readonly options: Options = {}) {
    this.watcher = chokidar.watch(this.directory, {
      ignored: ['**/node_modules/**/*', '**/.git/**/*'],
    });

    this.deployDirectory = new DeployDirectory(this.directory);

    this.uploader = new UploadService(this.config, this.directory, {
      isWatchMode: true,
      uploadEnv: true,
    });
  }

  public async watch() {
    await this.startDevMode();
    await this.upload();
    this.fetchStatus().catch(() => undefined);

    const onEvent = debounce(() => {
      this.upload().then(() => this.fetchStatus());
    }, 2_000);

    this.watcher.on('all', onEvent);

    setInterval(async () => {
      this.pollStatus().catch(console.error);
    }, 1_000);
  }

  private async pollStatus() {
    const response = await this.fetchStatus();
    const contentHash = await this.deployDirectory.contentHash();

    if (response?.contentHash === contentHash) {
      this.options.onBranchStatusChanged?.(response);
    }
  }

  private async startDevMode() {
    const response = await this.config.cloudReq({
      url: (deploymentId: string) => `build/devmode/${deploymentId}/start`,
      method: 'POST',
    });

    this.options.onDevModeStarted?.();

    return response;
  }

  private async fetchStatus() {
    try {
      return this.config.cloudReq<DevModeStatus>({
        url: (deploymentId: string) => `devmode/${deploymentId}/status`,
        method: 'GET',
      });
    } catch (error) {
      console.error('fetchStatus', error);
    }

    return null;
  }

  private async upload() {
    try {
      this.options.onWillUpload?.();
      await this.uploader.upload();
      this.options.onDidUpload?.();
    } catch (error) {
      console.error('Error', error);
    }
  }
}
