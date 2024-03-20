import type { OrchestratorApi } from './OrchestratorApi';
import {
  DatabaseType,
  DbTypeAsyncFn,
  DriverConfig,
  DriverContext,
  DriverFactoryAsyncFn,
  OrchestratorInitedOptions,
} from './types';
import { BaseDriver } from '@cubejs-backend/base-driver';
import { isDriver } from './DriverResolvers';
import { assertDataSource } from '@cubejs-backend/shared';

export class OrchestratorStorage {
  protected readonly resolvedConfigs: Map<string, Promise<any>> = new Map();

  protected readonly resolvedDrivers: Map<string, Promise<any>> = new Map();

  protected readonly storage: Map<string, OrchestratorApi> = new Map();

  public constructor(
    protected readonly dbType: DbTypeAsyncFn,
    protected readonly driverFactory: DriverFactoryAsyncFn,
  ) {
  }

  public has(orchestratorId: string) {
    return this.storage.has(orchestratorId);
  }

  public get(orchestratorId: string) {
    return this.storage.get(orchestratorId);
  }

  public set(orchestratorId: string, orchestratorApi: OrchestratorApi) {
    return this.storage.set(orchestratorId, orchestratorApi);
  }

  // protected async driverFactory(orchestratorId: string, ctx: DriverContext): Promise<DatabaseType> {
  //
  // }

  public async getDbType(orchestratorId: string, ctx: DriverContext): Promise<DatabaseType> {
    const key = `${orchestratorId}_${ctx.dataSource}`;

    const resolvedConfig = await this.resolvedConfigs.get(key);
    if (resolvedConfig) {
      return resolvedConfig.type;
    }

    const dbType = await this.dbType(ctx);

    return dbType;
  }

  public getDriver(orchestratorId: string, dataSource: string) {
    throw new Error('test');
  }

  // public async resolveDriver(
  //   context: DriverContext,
  //   options?: OrchestratorInitedOptions,
  // ): Promise<BaseDriver> {
  //   const val = await this.options.driverFactory(context);
  //   if (isDriver(val)) {
  //     return <BaseDriver>val;
  //   } else {
  //     const { type, ...rest } = <DriverConfig>val;
  //     const opts = Object.keys(rest).length
  //       ? rest
  //       : {
  //         maxPoolSize:
  //           await CubejsServerCore.getDriverMaxPool(context, options),
  //         testConnectionTimeout: options?.testConnectionTimeout,
  //       };
  //     opts.dataSource = assertDataSource(context.dataSource);
  //     return CubejsServerCore.createDriver(type, opts);
  //   }
  // }

  public clear() {
    this.storage.clear();
  }

  public async testConnections() {
    const result = [];

    // eslint-disable-next-line no-restricted-syntax
    for (const orchestratorApi of this.storage.values()) {
      result.push(orchestratorApi.testConnection());
    }

    return Promise.all(result);
  }

  public async testOrchestratorConnections() {
    const result = [];

    // eslint-disable-next-line no-restricted-syntax
    for (const orchestratorApi of this.storage.values()) {
      result.push(orchestratorApi.testOrchestratorConnections());
    }

    return Promise.all(result);
  }

  public async releaseConnections() {
    const result = [];

    // eslint-disable-next-line no-restricted-syntax
    for (const orchestratorApi of this.storage.values()) {
      result.push(orchestratorApi.release());
    }

    await Promise.all(result);

    this.storage.clear();
  }
}
