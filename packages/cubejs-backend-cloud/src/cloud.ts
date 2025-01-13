import fetch, { RequestInit } from 'node-fetch';
import FormData from 'form-data';
import path from 'path';
import { ReadStream } from 'fs';
import { DotenvParseOutput } from '@cubejs-backend/dotenv';

export type AuthObject = {
  auth: string,
  url?: string,
  deploymentId?: string
};

export interface StartUploadResponse {
  transaction: string;
  deploymentName: string;
}

export interface UpstreamHashesResponse {
  [key: string]: {
    hash: string;
  };
}

export class CubeCloudClient {
  public constructor(
    protected readonly auth?: AuthObject,
    protected readonly livePreview?: boolean
  ) {
  }

  private async request<T>(options: {
    url: (deploymentId: string) => string,
    auth?: AuthObject,
  } & RequestInit): Promise<T> {
    const { url, auth, ...restOptions } = options;

    const authorization = auth || this.auth;
    if (!authorization) {
      throw new Error('Auth isn\'t set');
    }
    // Ensure headers object exists in restOptions
    restOptions.headers = restOptions.headers || {};
    // Add authorization to headers
    (restOptions.headers as any).authorization = authorization.auth;
    (restOptions.headers as any)['Content-type'] = 'application/json';

    const response = await fetch(
      `${authorization.url}/${url(authorization.deploymentId || '')}`,
      restOptions,
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json() as Promise<T>;
  }

  public getDeploymentsList({ auth }: { auth?: AuthObject } = {}) {
    return this.request({
      url: () => 'build/deploy/deployments',
      method: 'GET',
      auth
    });
  }

  public async getDeploymentToken(authToken: string) {
    const response = await fetch(
      `${process.env.CUBE_CLOUD_HOST || 'https://cubecloud.dev'}/v1/token`,
      {
        method: 'POST',
        headers: { 'Content-type': 'application/json' },
        body: JSON.stringify({ token: authToken })
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const res = await response.json() as any;

    if (!res.jwt) {
      throw new Error('JWT token is not present in the response');
    }

    return res.jwt;
  }

  private extendRequestByLivePreview() {
    return this.livePreview ? '?live=true' : '';
  }

  public getUpstreamHashes({ auth }: { auth?: AuthObject } = {}): Promise<UpstreamHashesResponse> {
    return this.request({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/files${this.extendRequestByLivePreview()}`,
      method: 'GET',
      auth,
    });
  }

  public startUpload({ auth }: { auth?: AuthObject } = {}): Promise<StartUploadResponse> {
    return this.request({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/start-upload${this.extendRequestByLivePreview()}`,
      method: 'POST',
      auth,
    });
  }

  public uploadFile(
    { transaction, fileName, data, auth }:
      { transaction: any, fileName: string, data: ReadStream, auth?: AuthObject }
  ) {
    const formData = new FormData();
    formData.append('transaction', JSON.stringify(transaction));
    formData.append('fileName', fileName);
    formData.append('file', data, {
      filename: path.basename(fileName),
      contentType: 'application/octet-stream'
    });

    // Get the form data buffer and headers
    const formDataHeaders = formData.getHeaders();

    return this.request({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/upload-file${this.extendRequestByLivePreview()}`,
      method: 'POST',
      body: formData,
      headers: {
        ...formDataHeaders,
      },
      auth,
    });
  }

  public finishUpload({ transaction, files, auth }:
    { transaction: any, files: any, auth?: AuthObject }) {
    return this.request({
      url: (deploymentId: string) => `build/deploy/${deploymentId}/finish-upload${this.extendRequestByLivePreview()}`,
      method: 'POST',
      body: JSON.stringify({ transaction, files }),
      headers: {
        'Content-type': 'application/json'
      },
      auth,
    });
  }

  public setEnvVars({ envVariables, auth, replaceEnv }: { envVariables: DotenvParseOutput, auth?: AuthObject, replaceEnv?: boolean }) {
    const params = new URLSearchParams({ replaceEnv: Boolean(replaceEnv).toString() });
    return this.request({
      url: (deploymentId) => `build/deploy/${deploymentId}/set-env?${params.toString()}`,
      method: 'POST',
      body: JSON.stringify({ envVariables: JSON.stringify(envVariables) }),
      headers: {
        'Content-type': 'application/json'
      },
      auth
    });
  }

  public getStatusDevMode({ auth, lastHash }: { auth?: AuthObject, lastHash?: string } = {}): Promise<{[key: string]: any}> {
    const params = new URLSearchParams();
    if (lastHash) {
      params.append('lastHash', lastHash);
    }

    return this.request({
      url: (deploymentId) => `devmode/${deploymentId}/status?${params.toString()}`,
      method: 'GET',
      auth
    });
  }

  public createTokenDevMode({ auth, payload }: { auth?: AuthObject, payload?: Record<string, any> } = {}) {
    return this.request({
      url: (deploymentId) => `devmode/${deploymentId}/token`,
      method: 'POST',
      body: JSON.stringify(payload),
      headers: {
        'Content-type': 'application/json'
      },
      auth
    });
  }
}
