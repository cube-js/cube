import os from 'os';
import { CommandInterface } from './command.interface';

export class DiagnosticCommand implements CommandInterface {
  public getName() {
    return 'diagnostic';
  }

  public getDescription() {
    return 'Print diagnostic information about Cube.js setup to help creating an issue';
  }

  public async execute() {
    console.log(`Node: ${process.version}`);
    console.log(`OS: ${os.platform()}`);
  }
}
