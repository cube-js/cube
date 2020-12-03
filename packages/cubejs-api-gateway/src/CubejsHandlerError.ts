export class CubejsHandlerError extends Error {
  public constructor(
    public readonly status: number,
    public readonly type: string,
    message: string
  ) {
    super(message || type);
  }
}
