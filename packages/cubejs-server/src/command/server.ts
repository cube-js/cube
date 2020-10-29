import { Command, flags } from '@oclif/command';
import { ServerContainer } from '../server/container';

export class Server extends Command {
  static description = 'Run server in Production mode';

  static flags = {
    debug: flags.boolean({
      default: false,
      description: 'Print useful debug information'
    })
  }

  static args = [];

  async run() {
    const options = this.parse(Server);

    process.env.NODE_ENV = 'production';

    const container = new ServerContainer({
      debug: options.flags.debug,
    });

    const configuration = await container.lookupConfiguration();
    container.runServerInstance(configuration);
  }
}

export default Server;
