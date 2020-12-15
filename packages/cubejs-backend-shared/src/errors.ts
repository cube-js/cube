import { getEnv } from './env';

export function internalExceptions(e: Error) {
  const env = getEnv('internalExceptions');

  if (env !== 'false') {
    console.error(e);
  }

  if (env === 'exit') {
    process.exit(1);
  }
}
