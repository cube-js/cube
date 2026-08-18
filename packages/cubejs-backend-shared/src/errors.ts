import { getEnv } from './env';

export function internalExceptions(e: Error) {
  const env = getEnv('internalExceptions');

  if (env !== 'false') {
    console.error(e);
  }

  if (env === 'exit') {
    process.exit(1);
  }
}

/**
 * An error caused by an unsuccessful response from an HTTP API.
 *
 * The `isApiError` own property is a marker that allows detecting such errors
 * across package boundaries, where `instanceof` isn't reliable because of
 * possibly duplicated/mismatched copies of this package.
 */
export class ApiError extends Error {
  public readonly isApiError = true;

  public constructor(
    message: string,
    public readonly status?: number,
    public readonly url?: string,
    public readonly response?: string,
  ) {
    super(message);

    this.name = 'ApiError';
  }
}

export function isApiError(e: any): e is ApiError {
  return Boolean(e) && (e instanceof ApiError || e.isApiError === true);
}
