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

    // ServerContainer turns this into CreateOptions.devServer, after dotenv, so that an
    // explicit CUBEJS_DEV_MODE — from the environment or .env — still wins. It stays out
    // of process.env: that variable also gates the SQL API's port and password check
    const container = new ServerContainer({
      debug: options.flags.debug,
      devMode: true,
    });
    await container.runProjectDiagnostics();
    await container.start();
  }
}

export default DevServer;
