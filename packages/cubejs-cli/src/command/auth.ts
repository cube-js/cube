import type { CommanderStatic } from 'commander';
import { Config } from '@cubejs-backend/cloud';

import { displayError, event } from '../utils';

const authenticate = async (currentToken: string) => {
  const config = new Config();
  await config.addAuthToken(currentToken);

  await event({
    event: 'Cube Cloud CLI Authenticate'
  });

  console.log('Token successfully added!');
};

export function configureAuthCommand(program: CommanderStatic): void {
  program
    .command('auth <token>')
    .description('Authenticate access to Cube Cloud')
    .action(
      (currentToken) => authenticate(currentToken)
        .catch(e => displayError(e.stack || e))
    )
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs auth eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkZXBsb3ltZW50SWQiOiIxIiwidXJsIjoiaHR0cHM6Ly9leGFtcGxlcy5jdWJlY2xvdWQuZGV2IiwiaWF0IjoxNTE2MjM5MDIyfQ.La3MiuqfGigfzADl1wpxZ7jlb6dY60caezgqIOoHt-c');
      console.log('  $ cubejs deploy');
    });
}
