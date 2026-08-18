import fetch from 'node-fetch';
import { isApiError } from '@cubejs-backend/shared';
import { CubeCloudClient } from '../src/cloud';

jest.mock('node-fetch', () => jest.fn());

const fetchMock = fetch as unknown as jest.Mock;

test('CubeCloudClient: constuctor', async () => {
  const cubeCloudClient = new CubeCloudClient({
    auth: '',
    url: '',
    deploymentId: ''
  });
  expect(cubeCloudClient).not.toBeUndefined();
});

test('CubeCloudClient: throws ApiError for an unsuccessful response', async () => {
  fetchMock.mockResolvedValue({
    ok: false,
    status: 502,
    text: async () => 'Bad Gateway',
  });

  const cubeCloudClient = new CubeCloudClient({
    auth: 'token',
    url: 'https://cubecloud.dev',
    deploymentId: '1'
  });

  const error = await cubeCloudClient.getDeploymentsList().then(() => null, (e: any) => e);

  expect(isApiError(error)).toBe(true);
  expect(error.message).toContain('HTTP error! status: 502');
  expect(error.status).toBe(502);
  expect(error.url).toBe('https://cubecloud.dev/build/deploy/deployments');
  expect(error.response).toBe('Bad Gateway');
});
