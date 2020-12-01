import { get } from 'env-var';

const variables = {
  devMode: () => get('CUBEJS_DEV_MODE')
    .default('false')
    .asBoolStrict(),
  port: () => get('PORT')
    .default(4000)
    .required()
    .asPortNumber(),
  tlsPort: () => get('TLS_PORT')
    .default(4433)
    .required()
    .asPortNumber(),
  tls: () => get('CUBEJS_ENABLE_TLS')
    .default('false')
    .asBoolStrict(),
  webSockets: () => get('CUBEJS_WEB_SOCKETS')
    .default('false')
    .asBoolStrict(),
  // It's only excepted for CI, nothing else.
  internalExceptions: () => get('INTERNAL_EXCEPTIONS_YOU_WILL_BE_FIRED')
    .default('false')
    .asEnum(['exit', 'log', 'false'])
};

type Vars = typeof variables;

export function getEnv<T extends keyof Vars>(key: T): ReturnType<Vars[T]> {
  return <any>variables[key]();
}

export function isDockerImage(): boolean {
  return Boolean(process.env.CUBEJS_DOCKER_IMAGE_TAG);
}
