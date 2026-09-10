import { GenericContainer, ImageName, getContainerRuntimeClient } from 'testcontainers';
import { pausePromise } from '@cubejs-backend/shared';

export interface StartContainerRetryOptions {
  attempts?: number;
  delay?: number;
}

// Subclass instead of a cast to any: an upstream rename of `imageName` fails the build.
class ImageNameReader extends GenericContainer {
  public static of(container: GenericContainer): ImageName {
    return (container as ImageNameReader).imageName;
  }
}

const PERMANENT_FAILURES = [
  /manifest unknown/i,
  /manifest for .+ not found/i,
  /repository .+ not found/i,
  /pull access denied/i,
  /authentication required/i,
  /invalid reference format/i,
];

function isPermanent(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  const httpCode = message.match(/\(HTTP code (\d{3})\)/);

  if (httpCode) {
    const status = parseInt(httpCode[1], 10);

    return status >= 400 && status < 500 && status !== 429;
  }

  return PERMANENT_FAILURES.some(pattern => pattern.test(message));
}

// Pulling here rather than in start() keeps the retry on the pull: a container that
// fails its wait strategy is a real failure and restarting it only burns the timeout.
export async function pullImageWithRetry(imageName: ImageName, options: StartContainerRetryOptions = {}) {
  const attempts = options.attempts
    ?? parseInt(process.env.TEST_CONTAINER_PULL_ATTEMPTS || '3', 10);
  const delay = options.delay
    ?? parseInt(process.env.TEST_CONTAINER_PULL_RETRY_DELAY || '5000', 10);

  const client = await getContainerRuntimeClient();

  for (let attempt = 1; ; attempt++) {
    try {
      await client.image.pull(imageName, { force: false, platform: undefined });
      return;
    } catch (error) {
      if (attempt >= attempts || isPermanent(error)) {
        throw error;
      }

      const backoff = delay * (2 ** (attempt - 1));
      console.warn(
        `[testcontainers] pull ${imageName.string} attempt ${attempt}/${attempts} failed, retrying in ${backoff}ms:`,
        error instanceof Error ? error.message : error,
      );

      await pausePromise(backoff);
    }
  }
}

export async function startContainerWithRetry<C extends GenericContainer>(
  container: C,
  options: StartContainerRetryOptions = {},
): Promise<Awaited<ReturnType<C['start']>>> {
  await pullImageWithRetry(ImageNameReader.of(container), options);

  return container.start() as Promise<Awaited<ReturnType<C['start']>>>;
}
