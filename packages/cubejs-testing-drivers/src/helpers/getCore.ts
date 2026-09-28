import fs from 'fs-extra';
import path from 'path';
import { BaseDriver } from '@cubejs-backend/base-driver';
import { CubejsServerCore, DatabaseType } from '@cubejs-backend/server-core';
import { CubejsServerCoreExposed } from '../types/CubejsServerCoreExposed';

export function getCore(
  sourceType: string,
  storageType: string,
  source: BaseDriver,
  storage: BaseDriver,
): CubejsServerCoreExposed {
  const _path = path.resolve(process.cwd(), './.temp/model/ecommerce.yaml');
  // CreateOptions.dbType has been removed; the driver type for a driverFactory
  // returning a BaseDriver instance is resolved from CUBEJS_DB_TYPE.
  process.env.CUBEJS_DB_TYPE = sourceType;
  return new CubejsServerCore({
    apiSecret: 'mysupersecret',
    // devServer: true,
    scheduledRefreshTimer: 0,
    logger: (msg: string, params: Record<string, any>) => {
      // process.stdout.write(`${msg}\n${JSON.stringify(params, undefined, 2)}\n`);
    },
    driverFactory: async () => source,
    externalDbType: <DatabaseType>storageType,
    externalDriverFactory: async () => storage,
    repositoryFactory: () => ({
      localPath: () => __dirname,
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'ecommerce.yaml',
          content: fs.readFileSync(_path, 'utf8'),
        },
      ]),
    }),
  }) as unknown as CubejsServerCoreExposed;
}
