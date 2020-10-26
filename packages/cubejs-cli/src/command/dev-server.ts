import type { CommanderStatic } from 'commander';
import { proxyCommand } from './proxy-command';
import { displayError } from '../utils';

export function configureDevServerCommand(program: CommanderStatic) {
  return proxyCommand(program, 'dev-server')
    .catch(e => displayError(e.stack || e));
}
