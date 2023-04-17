import { CubejsServerCore } from './Core';

export type CubejsServerCoreExposed = CubejsServerCore & {
  options: any;
  optsHandler: any;
  contextToDbType: any;
  contextToExternalDbType: any;
  apiGateway: any;
  reloadEnvVariables: any;
  refreshScheduler: any;
  getRefreshScheduler: any;
};
