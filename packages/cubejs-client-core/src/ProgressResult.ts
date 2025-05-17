import { ProgressResponse } from './types';

export default class ProgressResult {
  private progressResponse: ProgressResponse;

  public constructor(progressResponse: ProgressResponse) {
    this.progressResponse = progressResponse;
  }

  public stage(): string {
    return this.progressResponse.stage;
  }

  public timeElapsed(): number {
    return this.progressResponse.timeElapsed;
  }
}
