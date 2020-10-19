import type { CommanderStatic } from 'commander';
import { proxyCommand } from './proxy-command';

export async function configureServerCommand(program: CommanderStatic) {
  return proxyCommand(program, 'server');
}
