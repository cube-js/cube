import { Command, flags } from '@oclif/command'
import { defaultExpiry, token } from '../token';

export class Token extends Command {
  static description = 'Create JWT token';

  static flags = {
    expiry: flags.string({
      name: 'expiry',
      char: 'e',
      description: (
        'Token expiry. Set to 0 for no expiry'
      ),
      default: defaultExpiry,
      required: true,
    }),
    secret: flags.string({
      name: 'secret',
      char: 's',
      description: (
        'Cube.js app secret. Also can be set via environment variable CUBEJS_API_SECRET'
      ),
      required: true,
    }),
    payload: flags.string({
      name: 'payload',
      char: 'p',
      description: (
        'Payload. Example: -p foo=bar'
      ),
    }),
  }

  static args = [];

  public async run() {
    const { args, flags } = this.parse(Token);

    await token(flags);
  }
}
