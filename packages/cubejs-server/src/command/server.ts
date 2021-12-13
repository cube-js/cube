import { Command, flags } from '@oclif/command';
import { ServerContainer } from '../server/container';

export class Server extends Command {
  public static description = 'Run server in Production mode';

  public static flags = {
    debug: flags.boolean({
      default: false,
      description: 'Print useful debug information'
    })
  };

  public static args = [];

  public async run() {
    const options = this.parse(Server);

    process.env.NODE_ENV = 'production';

    const container = new ServerContainer({
      debug: options.flags.debug,
    });
    await container.runProjectDiagnostics();
    await container.start();
  }
}

export default Server;
