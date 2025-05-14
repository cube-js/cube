export default class RequestError extends Error {
  response: any;

  status: number;

  public constructor(message: string, response: any, status: number) {
    super(message);
    this.response = response;
    this.status = status;
  }
}
