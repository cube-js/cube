import fs from 'fs-extra';
import jwt from 'jsonwebtoken';
import path from 'path';
import os from 'os';
import dotenv from '@cubejs-backend/dotenv';
import { isFilePath } from '@cubejs-backend/shared';

import { CubeCloudClient } from './cloud';

type ConfigurationFull = {
  auth: {
    [organizationUrl: string]: {
      auth: string,
    }
  },
  live: {
    [organizationUrl: string]: {
      [deploymentBranchKey: string]: {
        auth: string,
      }
    }
  }
};

type ConfigOptions = {
  directory?: string,
  home?: string
};

type Configuration = Partial<ConfigurationFull>;

export class Config {
  protected directory: string = process.cwd();

  protected home: string = os.homedir();

  protected readonly cubeCloudClient = new CubeCloudClient();

  public constructor(options: ConfigOptions = {}) {
    this.directory = options.directory || this.directory;
    this.home = options.home || this.home;
  }

  public async envFile(envFile: string) {
    if (await fs.pathExists(envFile)) {
      const env = dotenv.config({ path: envFile, multiline: 'line-breaks' }).parsed;
      if (env) {
        if ('CUBEJS_DEV_MODE' in env) {
          delete env.CUBEJS_DEV_MODE;
        }

        const resolvePossibleFiles = [
          'CUBEJS_DB_SSL_CA',
          'CUBEJS_DB_SSL_CERT',
          'CUBEJS_DB_SSL_KEY',
        ];

        // eslint-disable-next-line no-restricted-syntax
        for (const [key, value] of Object.entries(env)) {
          if (resolvePossibleFiles.includes(key) && isFilePath(value)) {
            if (fs.existsSync(value)) {
              env[key] = fs.readFileSync(value, 'ascii');
            } else {
              console.warn(`Unable to resolve file "${value}" from ${key}`);

              env[key] = '';
            }
          }
        }

        return env;
      }
    }

    return {};
  }

  public async addAuthToken(authToken: string, config?: Configuration): Promise<ConfigurationFull> {
    if (!config) {
      config = await this.loadConfig();
    }

    const payload = jwt.decode(authToken);
    if (payload && typeof payload === 'object' && payload.url) {
      config.auth = config.auth || {};
      config.auth[payload.url] = {
        auth: authToken
      };

      if (payload.deploymentId) {
        const dotCubeCloud = await this.loadDotCubeCloud();
        dotCubeCloud.url = payload.url;
        dotCubeCloud.deploymentId = payload.deploymentId;
        await this.writeDotCubeCloud(dotCubeCloud);
      }

      await this.writeConfig(config);
      return <ConfigurationFull>config;
    }

    const answer = await this.cubeCloudClient.getDeploymentToken(authToken);
    if (answer) {
      return this.addAuthToken(answer, config);
    }

    // eslint-disable-next-line no-throw-literal
    throw 'Malformed Cube Cloud token';
  }

  public async addLivePreviewToken(authToken: string, config?: Configuration): Promise<ConfigurationFull> {
    if (!config) {
      config = await this.loadConfig();
    }

    const payload = jwt.decode(authToken);
    if (
      payload &&
      typeof payload === 'object' &&
      payload.url &&
      payload.dId &&
      payload.branch &&
      payload.dUrl
    ) {
      // Set to home config
      const deploymentBranchKey = [payload.dId, payload.branch].join('/');
      config.live = config.live || {};
      config.live[payload.url] = config.live[payload.url] || {};
      config.live[payload.url][deploymentBranchKey] = {
        auth: authToken
      };

      // Set to project config
      const dotCubeCloud = await this.loadDotCubeCloud();
      dotCubeCloud.live = dotCubeCloud.live || [];
      dotCubeCloud.live.push(deploymentBranchKey);

      await this.writeDotCubeCloud(dotCubeCloud);
      await this.writeConfig(config);
      return <ConfigurationFull>config;
    }

    // eslint-disable-next-line no-throw-literal
    throw 'Malformed Cube Cloud live token';
  }

  public async loadConfig(): Promise<Configuration> {
    const { configFile } = this.configFile();

    if (await fs.pathExists(configFile)) {
      return fs.readJson(configFile);
    }

    return {};
  }

  protected async writeConfig(config: any) {
    const { cubeCloudConfigPath, configFile } = this.configFile();
    await fs.mkdirp(cubeCloudConfigPath);
    await fs.writeJson(configFile, config);
  }

  protected configFile() {
    const cubeCloudConfigPath = this.cubeCloudConfigPath();
    const configFile = path.join(cubeCloudConfigPath, 'config.json');

    return { cubeCloudConfigPath, configFile };
  }

  protected cubeCloudConfigPath() {
    return path.join(this.home, '.cubecloud');
  }

  protected dotCubeCloudFile() {
    return path.join(this.directory, '.cubecloud');
  }

  protected async loadDotCubeCloud() {
    if (await fs.pathExists(this.dotCubeCloudFile())) {
      return fs.readJson(this.dotCubeCloudFile());
    }

    return {};
  }

  protected async writeDotCubeCloud(config: any) {
    await fs.writeJson(this.dotCubeCloudFile(), config);
  }
}
