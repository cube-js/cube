import rp, { RequestPromiseOptions } from 'request-promise';
import path from 'path';
import { ReadStream } from 'node:fs';

export type AuthObject = {
  auth: string,
  url?: string,
  deploymentId?: string
};

export class CubeCloudClient {
  public constructor(
    protected readonly auth?: AuthObject,
    protected readonly livePreview?: Boolean
  ) {
  }

  private async request(options: {
    url: (deploymentId: string) => string,
    auth?: AuthObject,
  } & RequestPromiseOptions) {
    const { url, auth, ...restOptions } = options;

    const authorization = auth || this.auth;
    if (!authorization) {
      throw new Error('Auth isn\'t set');
    }

    return rp({
      headers: {
        authorization: authorization.auth
      },
      ...restOptions,
      url: `${authorization.url}/${url(authorization.deploymentId || '')}`,
      json: true
    });
  }

  public getDeploymentsList({ auth }: { auth?: AuthObject } = {}) {
    return this.request({
      url: () => 'build/deploy/deployments',
      method: 'GET',
      auth
    });
  }

  public async getDeploymentToken(authToken: string) {
    const res = await rp({
      url: `${process.env.CUBE_CLOUD_HOST || 'https://cubecloud.dev'}/v1/token`,
      method: 'POST',
      headers: {
        'Content-type': 'application/json'
      },
      json: true,
      body: {
        token: authToken
      }
    });

    if (res && res.error) {
      throw res.error;
    }

    return res.jwt;
  }

  private extendRequestByLivePreview() {
    return this.livePreview ? { qs: { live: 'true' } } : {};
  }

  public getUpstreamHashes({ auth }: { auth?: AuthObject } = {}) {
    return this.request({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/files`,
      method: 'GET',
      auth,
      ...this.extendRequestByLivePreview()
    });
  }

  public startUpload({ auth }: { auth?: AuthObject } = {}) {
    return this.request({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/start-upload`,
      method: 'POST',
      auth,
      ...this.extendRequestByLivePreview()
    });
  }

  public uploadFile(
    { transaction, fileName, data, auth }:
      { transaction: any, fileName: string, data: ReadStream, auth?: AuthObject }
  ) {
    return this.request({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/upload-file`,
      method: 'POST',
      formData: {
        transaction: JSON.stringify(transaction),
        fileName,
        file: {
          value: data,
          options: {
            filename: path.basename(fileName),
            contentType: 'application/octet-stream'
          }
        }
      },
      auth,
      ...this.extendRequestByLivePreview()
    });
  }

  public finishUpload({ transaction, files, auth }:
    { transaction: any, files: any, auth?: AuthObject }) {
    return this.request({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/finish-upload`,
      method: 'POST',
      body: {
        transaction,
        files
      },
      auth,
      ...this.extendRequestByLivePreview()
    });
  }

  public setEnvVars({ envVariables, auth }: { envVariables: any, auth?: AuthObject }) {
    return this.request({
      url: (deploymentId) => `build/deploy/${deploymentId}/set-env`,
      method: 'POST',
      body: {
        envVariables: JSON.stringify(envVariables),
      },
      auth
    });
  }

  public getStatusDevMode({ auth, lastHash }: { auth?: AuthObject, lastHash?: string } = {}) {
    return this.request({
      url: (deploymentId) => `devmode/${deploymentId}/status`,
      qs: { lastHash },
      method: 'GET',
      auth
    });
  }

  public createTokenDevMode({ auth, payload }: { auth?: AuthObject, payload?: Record<string, any> } = {}) {
    return this.request({
      url: (deploymentId) => `devmode/${deploymentId}/token`,
      method: 'POST',
      body: payload,
      auth
    });
  }
}
