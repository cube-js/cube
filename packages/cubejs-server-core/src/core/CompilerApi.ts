import type { SchemaFileRepository } from '@cubejs-backend/schema-compiler';

import crypto from 'crypto';
import { createQuery, compile, PreAggregations } from '@cubejs-backend/schema-compiler';

import type {
  DatabaseType,
  DbTypeFn,
  DialectContext,
  DialectFactoryFn,
  DriverContext,
  ExternalDbTypeFn,
  LoggerFn,
  PreAggregationFilter,
  RequestContext,
  SchemaVersionFn,
} from './types';

export interface CompilerApiOptions {
  allowJsDuplicatePropsInSchema?: boolean;
  allowNodeRequire?: boolean;
  allowUngroupedWithoutPrimaryKey?: boolean;
  compileContext?: RequestContext;
  dialectClass?: DialectFactoryFn;
  devServer?: boolean;
  externalDbType?: DatabaseType | ExternalDbTypeFn;
  externalDialectClass?: any;
  logger?: LoggerFn;
  preAggregationsSchema?: any;
  schemaVersion?: SchemaVersionFn;
  sqlCache?: boolean;
  standalone?: boolean;
}

export interface GetCompilersArgs {
  requestId?: string;
}

export interface GetSqlOptions {
  includeDebugInfo?: boolean;
}

export class CompilerApi {
  protected readonly dialectClass: DialectFactoryFn;

  protected readonly allowJsDuplicatePropsInSchema: boolean;

  protected readonly allowNodeRequire: boolean;

  protected readonly allowUngroupedWithoutPrimaryKey: boolean;

  protected readonly compileContext: RequestContext;

  protected readonly logger: LoggerFn;

  protected readonly sqlCache: boolean;

  protected readonly standalone: boolean;

  protected compilers: ReturnType<typeof compile>;

  protected compilerVersion: string;

  protected graphqlSchema: any;

  public readonly preAggregationsSchema: any;

  public schemaVersion: SchemaVersionFn;

  public constructor(
    protected repository: SchemaFileRepository,
    protected dbType: DatabaseType | DbTypeFn,
    protected options: CompilerApiOptions = {},
  ) {
    this.dialectClass = options.dialectClass;
    this.allowNodeRequire = options.allowNodeRequire == null ? true : options.allowNodeRequire;
    this.logger = this.options.logger;
    this.preAggregationsSchema = this.options.preAggregationsSchema;
    this.allowUngroupedWithoutPrimaryKey = this.options.allowUngroupedWithoutPrimaryKey;
    this.schemaVersion = this.options.schemaVersion;
    this.compileContext = options.compileContext;
    this.allowJsDuplicatePropsInSchema = options.allowJsDuplicatePropsInSchema;
    this.sqlCache = options.sqlCache;
    this.standalone = options.standalone;
  }

  public setGraphQLSchema(schema) {
    this.graphqlSchema = schema;
  }

  public getGraphQLSchema() {
    return this.graphqlSchema;
  }

  public async getCompilers({ requestId }: GetCompilersArgs = {}): Promise<ReturnType<typeof compile>> {
    let compilerVersion = (
      this.schemaVersion && await this.schemaVersion() ||
      'default_schema_version'
    );

    if (typeof compilerVersion === 'object') {
      compilerVersion = JSON.stringify(compilerVersion);
    }

    if (this.options.devServer) {
      const files = await this.repository.dataSchemaFiles();
      compilerVersion += `_${crypto.createHash('md5').update(JSON.stringify(files)).digest('hex')}`;
    }

    if (!this.compilers || this.compilerVersion !== compilerVersion) {
      this.logger(this.compilers ? 'Recompiling schema' : 'Compiling schema', {
        version: compilerVersion,
        requestId,
      });
      // TODO check if saving this promise can produce memory leak?
      this.compilers = compile(this.repository, {
        allowNodeRequire: this.allowNodeRequire,
        compileContext: this.compileContext,
        allowJsDuplicatePropsInSchema: this.allowJsDuplicatePropsInSchema,
        standalone: this.standalone,
      });
      this.compilerVersion = compilerVersion;
    }

    return this.compilers;
  }

  public getDbType(dataSource = 'default') {
    if (typeof this.dbType === 'function') {
      return this.dbType({ dataSource } as DriverContext);
    }

    return this.dbType;
  }

  public getDialectClass(dataSource = 'default', dbType: DatabaseType) {
    return this.dialectClass && this.dialectClass({ dataSource, dbType } as DialectContext);
  }

  public async getSql(query, options: GetSqlOptions = {}) {
    const { includeDebugInfo } = options;

    const dbType = this.getDbType();
    const compilers = await this.getCompilers({ requestId: query.requestId });
    let sqlGenerator = this.createQueryByDataSource(compilers, query);

    if (!sqlGenerator) {
      throw new Error(`Unknown dbType: ${dbType}`);
    }

    const dataSource = compilers.compiler.withQuery(sqlGenerator, () => sqlGenerator.dataSource);

    if (dataSource !== 'default' && dbType !== this.getDbType(dataSource)) {
      // TODO consider more efficient way than instantiating query
      sqlGenerator = this.createQueryByDataSource(
        compilers,
        query,
        dataSource,
      );

      if (!sqlGenerator) {
        throw new Error(`Can't find dialect for '${dataSource}' data source: ${this.getDbType(dataSource)}`);
      }
    }

    const getSqlFn = () => compilers.compiler.withQuery(sqlGenerator, () => ({
      external: sqlGenerator.externalPreAggregationQuery(),
      sql: sqlGenerator.buildSqlAndParams(),
      timeDimensionAlias: sqlGenerator.timeDimensions[0] && sqlGenerator.timeDimensions[0].unescapedAliasName(),
      timeDimensionField: sqlGenerator.timeDimensions[0] && sqlGenerator.timeDimensions[0].dimension,
      order: sqlGenerator.order,
      cacheKeyQueries: sqlGenerator.cacheKeyQueries(),
      preAggregations: sqlGenerator.preAggregations.preAggregationsDescription(),
      dataSource: sqlGenerator.dataSource,
      aliasNameToMember: sqlGenerator.aliasNameToMember,
      rollupMatchResults: includeDebugInfo ?
        sqlGenerator.preAggregations.rollupMatchResultDescriptions() : undefined,
      canUseTransformedQuery: sqlGenerator.preAggregations.canUseTransformedQuery(),
    }));

    if (this.sqlCache) {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { requestId, ...keyOptions } = query;
      const key = { query: keyOptions, options };
      return compilers.compilerCache.getQueryCache(key).cache(['sql'], getSqlFn);
    } else {
      return getSqlFn();
    }
  }

  public async preAggregations(filter?: PreAggregationFilter) {
    const { cubeEvaluator } = await this.getCompilers();
    return cubeEvaluator.preAggregations(filter);
  }

  public async scheduledPreAggregations() {
    const { cubeEvaluator } = await this.getCompilers();
    return cubeEvaluator.scheduledPreAggregations();
  }

  public createQueryByDataSource(compilers, query, dataSource?: string) {
    const dbType = this.getDbType(dataSource);

    return this.createQuery(compilers, dbType, this.getDialectClass(dataSource, dbType), query);
  }

  public createQuery(compilers, dbType, dialectClass, query) {
    return createQuery(
      compilers,
      dbType, {
        ...query,
        dialectClass,
        externalDialectClass: this.options.externalDialectClass,
        externalDbType: this.options.externalDbType,
        preAggregationsSchema: this.preAggregationsSchema,
        allowUngroupedWithoutPrimaryKey: this.allowUngroupedWithoutPrimaryKey,
      },
    );
  }

  public async metaConfig(options) {
    return (await this.getCompilers(options)).metaTransformer.cubes;
  }

  public canUsePreAggregationForTransformedQuery(transformedQuery, refs) {
    return PreAggregations.canUsePreAggregationForTransformedQueryFn(transformedQuery, refs);
  }
}
