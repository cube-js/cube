export default class RequestError extends Error {
  constructor(message, response, status) {
    super(message);
    this.response = response;
    this.status = status;
  }
}
