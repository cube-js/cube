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

    // Dev mode is driven by CUBEJS_DEV_MODE only, NODE_ENV is kept in sync for
    // user configuration code and third-party libraries that still read it
    process.env.CUBEJS_DEV_MODE = 'true';
    process.env.NODE_ENV = 'development';

    const container = new ServerContainer({
      debug: options.flags.debug,
    });
    await container.runProjectDiagnostics();
    await container.start();
  }
}

export default DevServer;
