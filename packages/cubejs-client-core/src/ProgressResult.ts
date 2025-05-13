export default class ProgressResult {
  constructor(progressResponse) {
    this.progressResponse = progressResponse;
  }

  stage() {
    return this.progressResponse.stage;
  }

  timeElapsed() {
    return this.progressResponse.timeElapsed;
  }
}
