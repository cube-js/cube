import { Command } from '@oclif/command';
import { ServerContainer } from '../server/container';

export class Server extends Command {
  static description = 'Run server in Production mode';

  static flags = {}

  static args = [];

  async run() {
    this.parse(Server);

    process.env.NODE_ENV = 'production';

    const container = new ServerContainer();
    const configuration = await container.lookupConfiguration();
    container.runServerInstance(configuration);
  }
}

export default Server;
