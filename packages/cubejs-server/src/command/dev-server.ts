import { Command } from '@oclif/command';
import { ServerContainer } from '../server/container';

export class DevServer extends Command {
  static description = 'Run server in Development mode';

  static flags = {}

  static args = [];

  async run() {
    this.parse(DevServer);

    process.env.NODE_ENV = 'development';

    const container = new ServerContainer();
    const configuration = await container.lookupConfiguration();
    container.runServerInstance(configuration);
  }
}

export default DevServer;
