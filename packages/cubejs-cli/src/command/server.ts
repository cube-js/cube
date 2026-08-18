import type { CommanderStatic } from 'commander';
import { proxyCommand } from './proxy-command';
import { displayError } from '../utils';

export async function configureServerCommand(program: CommanderStatic) {
  return proxyCommand(program, 'server')
    .catch(e => displayError(e.stack || e));
}
