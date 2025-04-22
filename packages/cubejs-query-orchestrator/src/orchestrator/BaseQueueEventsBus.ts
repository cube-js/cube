export class BaseQueueEventsBus {
  protected readonly subscribers: Record<string, any> = {};

  public subscribe(id: string, callback) {
    this.subscribers[id] = { id, callback };
  }

  public unsubscribe(id: string) {
    delete this.subscribers[id];
  }
}
