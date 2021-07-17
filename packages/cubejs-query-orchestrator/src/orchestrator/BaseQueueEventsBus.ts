export class BaseQueueEventsBus {
  protected readonly subscribers: Record<string, any>;

  public constructor() {
    this.subscribers = {};
  }

  public subscribe(id, callback) {
    this.subscribers[id] = { id, callback };
  }
  
  public unsubscribe(id) {
    delete this.subscribers[id];
  }
}
