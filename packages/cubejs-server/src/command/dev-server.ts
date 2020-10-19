import {Command, flags} from '@oclif/command';

export default class DevServer extends Command {
  static description = 'Run server in Development mode';

  static flags = {}

  static args = [];

  async run() {
    this.parse(DevServer);

    console.log('Demo, source code will be in next PR');
  }
}
