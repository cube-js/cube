export enum ServerStatus {
  UP = 'UP',
  SHUTDOWN = 'SHUTDOWN',
}

export class ServerStatusHandler {
  protected status: ServerStatus = ServerStatus.UP;

  public isUp(): boolean {
    return this.status === ServerStatus.UP;
  }

  public shutdown() {
    this.status = ServerStatus.SHUTDOWN;
  }
}
