export default class RequestError extends Error {
  public response: any;

  public status: number;

  public constructor(message: string, response: any, status: number, options?: { cause?: unknown }) {
    super(message, options);
    this.response = response;
    this.status = status;
  }
}
