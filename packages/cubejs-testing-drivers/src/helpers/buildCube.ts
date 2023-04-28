import path from 'path';
import { execInDir } from '@cubejs-backend/shared';

/**
 * Sync build testing-drivers image.
 */
export function buildCube(): void {
  const status = execInDir(
    path.resolve(process.cwd(), '../..'),
    'docker build . ' +
      '-f packages/cubejs-docker/testing-drivers.Dockerfile ' +
      // '-f packages/cubejs-docker/dev.Dockerfile ' +
      '-t cubejs/cube:testing-drivers',
  );
  if (status !== 0) {
    throw new Error('Docker build failed.');
  }
}
