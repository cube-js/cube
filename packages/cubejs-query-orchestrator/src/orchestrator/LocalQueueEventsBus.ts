import { BaseQueueEventsBus } from './BaseQueueEventsBus';

export class LocalQueueEventsBus extends BaseQueueEventsBus {
  protected readonly subscribers: Record<string, any>;

  public emit(event) {
    Promise.all(Object.values(this.subscribers).map(({ callback }) => callback(event)))
      .catch(err => console.error(err));
  }
}
