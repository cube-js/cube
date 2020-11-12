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
};

type Vars = typeof variables;

export function getEnv<T extends keyof Vars>(key: T): ReturnType<Vars[T]> {
  return <any>variables[key]();
}
