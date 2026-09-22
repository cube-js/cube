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

    // Dev mode is driven by CUBEJS_DEV_MODE only. It is defaulted in ServerContainer,
    // after dotenv, so that an explicit value — from the environment or .env — still
    // wins, and NODE_ENV is synced from it there
    const container = new ServerContainer({
      debug: options.flags.debug,
      devMode: true,
    });
    await container.runProjectDiagnostics();
    await container.start();
  }
}

export default DevServer;
