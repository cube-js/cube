import chalk from 'chalk';
import jwt from 'jsonwebtoken';
import { isDockerImage, requireFromPackage } from '@cubejs-backend/shared';
import type { CommanderStatic } from 'commander';

import { displayError, displayWarning, event } from '../utils';

export const defaultExpiry = '30 days';

const parsePayload = (payloadArray: string[] = []) => {
  const result = {};

  payloadArray.forEach((entry = '') => {
    const [key, value] = entry.split('=');
    if (key && value) {
      result[key] = value;
    }
  });

  return result;
};

type TokenOptions = {
  expiry?: string;
  secret?: string;
  expiresIn?: string
  payload: string[]
  userContext: string[]
};

export const token = async (options: TokenOptions) => {
  event({
    event: 'Generate Token'
  });

  const cubejsServer = requireFromPackage<any>('@cubejs-backend/server', {
    relative: isDockerImage()
  });
  const { expiry = defaultExpiry, secret = cubejsServer.apiSecret() } = options;

  if (!secret) {
    throw new Error('No app secret found');
  }

  const extraOptions: Record<string, string> = {};

  if (expiry !== '0') {
    extraOptions.expiresIn = expiry;
  }

  const payload = {
    ...parsePayload(options.payload),
  };

  const userContext = parsePayload(options.userContext);
  if (userContext) {
    displayWarning('Option --user-context was deprecated and payload will be stored inside root instead of u');

    // eslint-disable-next-line no-restricted-syntax
    for (const key of Object.keys(userContext)) {
      if (key in payload) {
        displayWarning(`Key ${key} already exists inside payload and will be overritten by user-context`);
      }

      payload[key] = userContext[key];
    }
  }

  console.log('Generating Cube.js JWT token');
  console.log('');
  console.log(`${chalk.yellow('-----------------------------------------------------------------------------------------')}`);
  console.log(`  ${chalk.yellow('Use these manually generated tokens in production with caution.')}`);
  console.log(`  ${chalk.yellow(`Please refer to ${chalk.cyan('https://cube.dev/docs/security')} for production security best practices.`)}`);
  console.log(`${chalk.yellow('-----------------------------------------------------------------------------------------')}`);
  console.log('');
  console.log(`Expires in: ${chalk.green(expiry)}`);
  console.log(`Payload: ${chalk.green(JSON.stringify(payload))}`);
  console.log('');

  const signedToken = jwt.sign(payload, secret, extraOptions);
  console.log(`Token: ${chalk.green(signedToken)}`);

  await event({
    event: 'Generate Token Success'
  });

  return signedToken;
};

export const collect = (val, memo) => [val, ...memo];

export function configureTokenCommand(program: CommanderStatic) {
  program
    .command('token')
    .option('-e, --expiry [expiry]', 'Token expiry. Set to 0 for no expiry')
    .option('-s, --secret [secret]', 'Cube.js app secret. Also can be set via environment variable CUBEJS_API_SECRET')
    .option('-p, --payload [values]', 'Payload. Example: -p foo=bar', collect, [])
    .option('-u, --user-context [values]', 'USER_CONTEXT. Example: -u baz=qux', collect, [])
    .description('Create JWT token')
    .action(
      (options) => token(options)
        .catch(e => displayError(e.stack || e))
    )
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs token -e "1 day" -p foo=bar -p cool=true');
    });
}
