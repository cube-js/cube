import { Command, flags } from '@oclif/command';

export default class Server extends Command {
  static description = 'Run server in Production mode';

  static flags = {}

  static args = [];

  async run() {
    this.parse(Server);

    console.log('Demo, source code will be in next PR');
  }
}
