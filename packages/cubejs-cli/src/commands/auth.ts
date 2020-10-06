import { Command, flags } from '@oclif/command'
import { event } from '../utils';
import { Config } from '../config';

export class Auth extends Command {
  static description = 'Generate Cube.js schema from DB tables schema';

  static flags = {
    token: flags.string({
      name: 'tables',
      char: 't',
      description: (
        'Comma delimited list of tables to generate schema from'
      ),
      required: true,
    }),
  }

  static args = [];

  public async run() {
    const { args, flags } = this.parse(Auth);

    const config = new Config();
    await config.addAuthToken(flags.token);
    await event('Cube Cloud CLI Authenticate');
    console.log('Token successfully added!');
  }
}
