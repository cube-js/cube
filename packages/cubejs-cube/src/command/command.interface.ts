export interface CommandInterface {
  getName(): string;
  getDescription(): string;
  execute(): Promise<any>;
}
