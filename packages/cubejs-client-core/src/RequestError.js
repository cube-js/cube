export default class RequestError extends Error {
  constructor(message, response) {
    super(message);
    this.response = response;
  }
}
