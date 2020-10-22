import type { CommanderStatic } from 'commander';
import { proxyCommand } from './proxy-command';

export function configureDevServerCommand(program: CommanderStatic) {
  return proxyCommand(program, 'dev-server');
}
