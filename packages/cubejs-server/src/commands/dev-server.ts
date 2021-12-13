import { Command, flags } from '@oclif/command';
import { ServerContainer } from '../server/container';

export class DevServer extends Command {
  public static description = 'Run server in Development mode';

  public static flags = {
    debug: flags.boolean({
      default: false,
      description: 'Print useful debug information'
    })
  };

  public static args = [];

  public async run() {
    const options = this.parse(DevServer);

    process.env.NODE_ENV = 'development';

    const container = new ServerContainer({
      debug: options.flags.debug,
    });
    await container.runProjectDiagnostics();
    await container.start();
  }
}

export default DevServer;
