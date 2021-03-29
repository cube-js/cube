import { CubeCloudClient } from '../src/cloud';

test('CubeCloudClient: constuctor', async () => {
  const cubeCloudClient = new CubeCloudClient({
    auth: '',
    url: '',
    deploymentId: '',
    deploymentUrl: ''
  });
  expect(cubeCloudClient).not.toBeUndefined();
});
