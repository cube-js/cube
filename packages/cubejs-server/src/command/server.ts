import { Command, flags } from '@oclif/command';
import { getEnv } from '@cubejs-backend/shared';
import { ServerContainer } from '../server/container';

export class Server extends Command {
  public static description = 'Run server in Production mode';

  public static flags = {
    debug: flags.boolean({
      default: false,
      description: 'Print useful debug information'
    })
  }

  public static args = [];

  public async run() {
    const options = this.parse(Server);

    process.env.NODE_ENV = 'production';

    const devMode = getEnv('devMode');
    if (devMode) {
      process.env.NODE_ENV = 'development';
    }

    const container = new ServerContainer({
      debug: options.flags.debug,
    });
    container.runProjectDiagnostics();

    const configuration = await container.lookupConfiguration();
    container.runServerInstance(configuration);
  }
}

export default Server;
