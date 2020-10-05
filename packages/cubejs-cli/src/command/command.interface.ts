import { CommanderStatic, Command } from 'commander';

export interface CommandInterface {
  configure(program: CommanderStatic): Command;
  execute(...args: any[]): Promise<any>;
}
