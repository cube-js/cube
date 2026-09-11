export class CubeClientError extends Error {
  public readonly cause: unknown;

  public readonly response: unknown;

  public readonly status: number | undefined;

  public constructor(message: string, cause?: unknown) {
    super(message);
    this.name = 'CubeClientError';
    this.cause = cause;
    this.response = (cause as { response?: unknown } | null)?.response;
    this.status = (cause as { status?: number } | null)?.status;
  }
}
export function normalizeCubeError(value: unknown): Error {
  const candidate = value as {
    message?: string;
    response?: { plainError?: string };
  } | null;
  const message =
    candidate?.response?.plainError ?? candidate?.message ?? String(value);

  if (value instanceof Error && value.message === message) {
    return value;
  }

  return new CubeClientError(message, value);
}

export function isAbortError(value: unknown): boolean {
  const candidate = value as { name?: string; message?: string } | null;

  return (
    candidate?.name === 'AbortError' ||
    candidate?.message === 'aborted' ||
    candidate?.message === 'The operation was aborted'
  );
}
