import { Command, flags } from '@oclif/command'
import { deploy } from '../deploy';

export class Deploy extends Command {
  static description = 'Deploy project to Cube Cloud';

  static flags = {}

  static args = [];

  public async run() {
    const { args, flags } = this.parse(Deploy);

    const options = { auth: 'test' };
    await deploy({ directory: process.cwd(), ...options })
  }
}
