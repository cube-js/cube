import crypto from 'crypto';
import {
  createQuery,
  compile,
  queryClass,
  PreAggregations,
  QueryFactory,
  prepareCompiler,
  BaseQuery,
} from '@cubejs-backend/schema-compiler';
import { v4 as uuidv4, parse as uuidParse } from 'uuid';
import { LRUCache } from 'lru-cache';
import { NativeInstance } from '@cubejs-backend/native';
import { SchemaFileRepository } from '@cubejs-backend/shared';
import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import {
  DbTypeAsyncFn,
  DatabaseType,
  DialectFactoryFn,
  DriverContext,
  DialectContext,
  RequestContext,
  LoggerFn,
} from './types';

interface CompilerApiOptions {
  dialectClass?: DialectFactoryFn;
  logger?: LoggerFn;
  preAggregationsSchema?: (context: RequestContext) => string | Promise<string>;
  allowUngroupedWithoutPrimaryKey?: boolean;
  convertTzForRawTimeDimension?: boolean;
  schemaVersion?: () => string | object | Promise<string | object>;
  contextToRoles?: (context: RequestContext) => string[] | Promise<string[]>;
  compileContext?: any;
  allowJsDuplicatePropsInSchema?: boolean;
  sqlCache?: boolean;
  standalone?: boolean;
  compilerCacheSize?: number;
  maxCompilerCacheKeepAlive?: number;
  updateCompilerCacheKeepAlive?: boolean;
  externalDialectClass?: typeof BaseQuery;
  externalDbType?: DatabaseType;
  allowNodeRequire?: boolean;
  devServer?: boolean;
  fastReload?: boolean;
}

interface CompilersResult {
  compiler: any;
  metaTransformer: any;
  cubeEvaluator: any;
  contextEvaluator: any;
  joinGraph: any;
  compilerCache: any;
  headCommitId: string;
  compilerId: string;
}

interface SqlGeneratorResult {
  external: any;
  sql: any;
  lambdaQueries: any;
  timeDimensionAlias?: string;
  timeDimensionField?: string;
  order: any;
  cacheKeyQueries: any;
  preAggregations: any;
  dataSource: string;
  aliasNameToMember: any;
  rollupMatchResults?: any;
  canUseTransformedQuery: any;
  memberNames: string[];
}

interface ApplicablePolicy {
  role: string;
  conditions?: Array<{ if: any }>;
  rowLevel?: {
    filters?: any[];
    allowAll?: boolean;
  };
  memberLevel?: {
    includesMembers: string[];
    excludesMembers: string[];
  };
}

interface NestedFilter {
  memberReference?: string;
  member?: string;
  operator?: string;
  values?: any;
  or?: NestedFilter[];
  and?: NestedFilter[];
}

interface CubeConfig {
  name: string;
  measures?: Array<{ name: string; isVisible?: boolean; public?: boolean }>;
  dimensions?: Array<{ name: string; isVisible?: boolean; public?: boolean }>;
  segments?: Array<{ name: string; isVisible?: boolean; public?: boolean }>;
  hierarchies?: Array<{ name: string; isVisible?: boolean; public?: boolean }>;
}

interface CubeWithConfig {
  config: CubeConfig;
}

export class CompilerApi {
  private repository: SchemaFileRepository;

  private dbType: DbTypeAsyncFn;

  private dialectClass?: DialectFactoryFn;

  public options: CompilerApiOptions;

  private allowNodeRequire: boolean;

  private logger?: LoggerFn;

  private preAggregationsSchema?: (context: RequestContext) => string | Promise<string>;

  private allowUngroupedWithoutPrimaryKey?: boolean;

  private convertTzForRawTimeDimension?: boolean;

  public schemaVersion?: () => string | object | Promise<string | object>;

  private contextToRoles?: (context: RequestContext) => string[] | Promise<string[]>;

  private compileContext?: any;

  private allowJsDuplicatePropsInSchema?: boolean;

  private sqlCache?: boolean;

  private standalone?: boolean;

  private nativeInstance: NativeInstance;

  private compiledScriptCache: LRUCache<string, any>;

  private compiledScriptCacheInterval?: NodeJS.Timeout;

  private graphqlSchema?: any;

  private compilers?: Promise<CompilersResult>;

  private compilerVersion?: string;

  private queryFactory?: QueryFactory;

  public constructor(
    repository: SchemaFileRepository,
    dbType: DbTypeAsyncFn,
    options: CompilerApiOptions = {}
  ) {
    this.repository = repository;
    this.dbType = dbType;
    this.dialectClass = options.dialectClass;
    this.options = options;
    this.allowNodeRequire = options.allowNodeRequire == null ? true : options.allowNodeRequire;
    this.logger = this.options.logger;
    this.preAggregationsSchema = this.options.preAggregationsSchema;
    this.allowUngroupedWithoutPrimaryKey = this.options.allowUngroupedWithoutPrimaryKey;
    this.convertTzForRawTimeDimension = this.options.convertTzForRawTimeDimension;
    this.schemaVersion = this.options.schemaVersion;
    this.contextToRoles = this.options.contextToRoles;
    this.compileContext = options.compileContext;
    this.allowJsDuplicatePropsInSchema = options.allowJsDuplicatePropsInSchema;
    this.sqlCache = options.sqlCache;
    this.standalone = options.standalone;
    this.nativeInstance = this.createNativeInstance();
    this.compiledScriptCache = new LRUCache({
      max: options.compilerCacheSize || 250,
      ttl: options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: options.updateCompilerCacheKeepAlive
    });

    if (this.options.maxCompilerCacheKeepAlive) {
      this.compiledScriptCacheInterval = setInterval(
        () => this.compiledScriptCache.purgeStale(),
        this.options.maxCompilerCacheKeepAlive
      );
    }
  }

  public dispose(): void {
    if (this.compiledScriptCacheInterval) {
      clearInterval(this.compiledScriptCacheInterval);
    }
  }

  public setGraphQLSchema(schema: any): void {
    this.graphqlSchema = schema;
  }

  public getGraphQLSchema(): any {
    return this.graphqlSchema;
  }

  public createNativeInstance(): NativeInstance {
    return new NativeInstance();
  }

  public async getCompilers({ requestId }: { requestId?: string } = {}): Promise<CompilersResult> {
    let compilerVersion = (
      this.schemaVersion && await this.schemaVersion() ||
      'default_schema_version'
    );

    if (typeof compilerVersion === 'object') {
      compilerVersion = JSON.stringify(compilerVersion);
    }

    if (this.options.devServer || this.options.fastReload) {
      const files = await this.repository.dataSchemaFiles();
      compilerVersion += `_${crypto.createHash('md5').update(JSON.stringify(files)).digest('hex')}`;
    }

    if (!this.compilers || this.compilerVersion !== compilerVersion) {
      this.compilers = this.compileSchema(compilerVersion, requestId).catch(e => {
        this.compilers = undefined;
        throw e;
      });
      this.compilerVersion = compilerVersion;
    }

    return this.compilers;
  }

  public createCompilerInstances(): CompilersResult {
    return prepareCompiler(this.repository, {
      allowNodeRequire: this.allowNodeRequire,
      compileContext: this.compileContext,
      allowJsDuplicatePropsInSchema: this.allowJsDuplicatePropsInSchema,
      standalone: this.standalone,
      nativeInstance: this.nativeInstance,
      compiledScriptCache: this.compiledScriptCache,
    });
  }

  public async compileSchema(compilerVersion: string, requestId?: string): Promise<CompilersResult> {
    const startCompilingTime = new Date().getTime();
    try {
      this.logger?.(this.compilers ? 'Recompiling schema' : 'Compiling schema', {
        version: compilerVersion,
        requestId
      });

      const compilers = await compile(this.repository, {
        allowNodeRequire: this.allowNodeRequire,
        compileContext: this.compileContext,
        allowJsDuplicatePropsInSchema: this.allowJsDuplicatePropsInSchema,
        standalone: this.standalone,
        nativeInstance: this.nativeInstance,
        compiledScriptCache: this.compiledScriptCache,
      });
      this.queryFactory = await this.createQueryFactory(compilers);

      this.logger?.('Compiling schema completed', {
        version: compilerVersion,
        requestId,
        duration: ((new Date()).getTime() - startCompilingTime),
      });

      return compilers;
    } catch (e: any) {
      this.logger?.('Compiling schema error', {
        version: compilerVersion,
        requestId,
        duration: ((new Date()).getTime() - startCompilingTime),
        error: (e.stack || e).toString()
      });
      throw e;
    }
  }

  public async createQueryFactory(compilers: CompilersResult): Promise<QueryFactory> {
    const { cubeEvaluator } = compilers;

    const cubeToQueryClass = Object.fromEntries(
      await Promise.all(
        cubeEvaluator.cubeNames().map(async (cube) => {
          const dataSource = cubeEvaluator.cubeFromPath(cube).dataSource ?? 'default';
          const dbType = await this.getDbType(dataSource);
          const dialectClass = this.getDialectClass(dataSource, dbType);
          return [cube, queryClass(dbType, dialectClass)];
        })
      )
    );
    return new QueryFactory(cubeToQueryClass);
  }

  public async getDbType(dataSource: string = 'default'): Promise<DatabaseType> {
    return this.dbType({ dataSource } as DriverContext);
  }

  public getDialectClass(dataSource: string = 'default', dbType: DatabaseType): any {
    return this.dialectClass?.({ dataSource, dbType } as DialectContext);
  }

  public async getSqlGenerator(query: any, dataSource?: string): Promise<{ sqlGenerator: any; compilers: CompilersResult }> {
    const dbType = await this.getDbType(dataSource);
    const compilers = await this.getCompilers({ requestId: query.requestId });
    let sqlGenerator = await this.createQueryByDataSource(compilers, query, dataSource, dbType);

    if (!sqlGenerator) {
      throw new Error(`Unknown dbType: ${dbType}`);
    }

    dataSource = compilers.compiler.withQuery(sqlGenerator, () => sqlGenerator.dataSource);
    if (dataSource !== undefined) {
      const _dbType = await this.getDbType(dataSource);
      if (dataSource !== 'default' && dbType !== _dbType) {
        sqlGenerator = await this.createQueryByDataSource(
          compilers,
          query,
          dataSource,
          _dbType
        );

        if (!sqlGenerator) {
          throw new Error(
            `Can't find dialect for '${dataSource}' data source: ${_dbType}`
          );
        }
      }
    }

    return { sqlGenerator, compilers };
  }

  public async getSql(
    query: any,
    options: { includeDebugInfo?: boolean; exportAnnotatedSql?: boolean; requestId?: string } = {}
  ): Promise<SqlGeneratorResult> {
    const { includeDebugInfo, exportAnnotatedSql } = options;
    const { sqlGenerator, compilers } = await this.getSqlGenerator(query);

    const getSqlFn = () => compilers.compiler.withQuery(sqlGenerator, () => ({
      external: sqlGenerator.externalPreAggregationQuery(),
      sql: sqlGenerator.buildSqlAndParams(exportAnnotatedSql),
      lambdaQueries: sqlGenerator.buildLambdaQuery(),
      timeDimensionAlias: sqlGenerator.timeDimensions[0]?.unescapedAliasName(),
      timeDimensionField: sqlGenerator.timeDimensions[0]?.dimension,
      order: sqlGenerator.order,
      cacheKeyQueries: sqlGenerator.cacheKeyQueries(),
      preAggregations: sqlGenerator.preAggregations.preAggregationsDescription(),
      dataSource: sqlGenerator.dataSource,
      aliasNameToMember: sqlGenerator.aliasNameToMember,
      rollupMatchResults: includeDebugInfo ?
        sqlGenerator.preAggregations.rollupMatchResultDescriptions() : undefined,
      canUseTransformedQuery: sqlGenerator.preAggregations.canUseTransformedQuery(),
      memberNames: sqlGenerator.collectAllMemberNames(),
    }));

    if (this.sqlCache) {
      const { requestId: _requestId, ...keyOptions } = query;
      const key = { query: keyOptions, options };
      return compilers.compilerCache.getQueryCache(key).cache(['sql'], getSqlFn);
    } else {
      return getSqlFn();
    }
  }

  public async getRolesFromContext(context: RequestContext): Promise<Set<string>> {
    if (!this.contextToRoles) {
      return new Set();
    }
    return new Set(await this.contextToRoles(context));
  }

  public userHasRole(userRoles: Set<string>, role: string): boolean {
    return userRoles.has(role) || role === '*';
  }

  public roleMeetsConditions(evaluatedConditions?: any[]): boolean {
    if (evaluatedConditions?.length) {
      return evaluatedConditions.reduce((a, b) => {
        if (typeof b !== 'boolean') {
          throw new Error(`Access policy condition must return boolean, got ${JSON.stringify(b)}`);
        }
        return a && b;
      });
    }
    return true;
  }

  public async getCubesFromQuery(query: any, context: RequestContext): Promise<Set<string>> {
    const sql = await this.getSql(query, { requestId: context.requestId });
    return new Set(sql.memberNames.map(memberName => memberName.split('.')[0]));
  }

  public hashRequestContext(context: any): string {
    if (!context.__hash) {
      context.__hash = crypto.createHash('md5').update(JSON.stringify(context)).digest('hex');
    }
    return context.__hash;
  }

  public async getApplicablePolicies(
    cube: any,
    context: RequestContext,
    compilers: CompilersResult
  ): Promise<ApplicablePolicy[]> {
    const cache = compilers.compilerCache.getRbacCacheInstance();
    const cacheKey = `${cube.name}_${this.hashRequestContext(context)}`;
    if (!cache.has(cacheKey)) {
      const userRoles = await this.getRolesFromContext(context);
      const policies = cube.accessPolicy.filter((policy: ApplicablePolicy) => {
        const evaluatedConditions = (policy.conditions || []).map(
          condition => compilers.cubeEvaluator.evaluateContextFunction(cube, condition.if, context)
        );
        const res = this.userHasRole(userRoles, policy.role) && this.roleMeetsConditions(evaluatedConditions);
        return res;
      });
      cache.set(cacheKey, policies);
    }
    return cache.get(cacheKey);
  }

  public evaluateNestedFilter(
    filter: any,
    cube: any,
    context: RequestContext,
    cubeEvaluator: any
  ): NestedFilter {
    const result: NestedFilter = {};
    
    if (filter.memberReference) {
      const evaluatedValues = cubeEvaluator.evaluateContextFunction(
        cube,
        filter.values || (() => undefined),
        context
      );
      result.member = filter.memberReference;
      result.operator = filter.operator;
      result.values = evaluatedValues;
    }
    if (filter.or) {
      result.or = filter.or.map((f: any) => this.evaluateNestedFilter(f, cube, context, cubeEvaluator));
    }
    if (filter.and) {
      result.and = filter.and.map((f: any) => this.evaluateNestedFilter(f, cube, context, cubeEvaluator));
    }
    return result;
  }

  public async applyRowLevelSecurity(
    query: any,
    evaluatedQuery: any,
    context: RequestContext
  ): Promise<{ query: any; denied: boolean }> {
    const compilers = await this.getCompilers({ requestId: context.requestId });
    const { cubeEvaluator } = compilers;

    if (!cubeEvaluator.isRbacEnabled()) {
      return { query, denied: false };
    }

    const queryCubes = await this.getCubesFromQuery(evaluatedQuery, context);

    const cubeFiltersPerCubePerRole: Record<string, Record<string, NestedFilter[]>> = {};
    const viewFiltersPerCubePerRole: Record<string, Record<string, NestedFilter[]>> = {};
    const hasAllowAllForCube: Record<string, boolean> = {};

    for (const cubeName of queryCubes) {
      const cube = cubeEvaluator.cubeFromPath(cubeName);
      const filtersMap = cube.isView ? viewFiltersPerCubePerRole : cubeFiltersPerCubePerRole;

      if (cubeEvaluator.isRbacEnabledForCube(cube)) {
        let hasRoleWithAccess = false;
        const userPolicies = await this.getApplicablePolicies(cube, context, compilers);

        for (const policy of userPolicies) {
          hasRoleWithAccess = true;
          (policy?.rowLevel?.filters || []).forEach((filter: any) => {
            filtersMap[cubeName] = filtersMap[cubeName] || {};
            filtersMap[cubeName][policy.role] = filtersMap[cubeName][policy.role] || [];
            filtersMap[cubeName][policy.role].push(
              this.evaluateNestedFilter(filter, cube, context, cubeEvaluator)
            );
          });
          if (!policy?.rowLevel || policy?.rowLevel?.allowAll) {
            hasAllowAllForCube[cubeName] = true;
            break;
          }
        }

        if (!hasRoleWithAccess) {
          query.segments = query.segments || [];
          query.segments.push({
            expression: () => '1 = 0',
            cubeName: cube.name,
            name: 'rlsAccessDenied',
          });
          return { query, denied: true };
        }
      }
    }

    const rlsFilter = this.buildFinalRlsFilter(
      cubeFiltersPerCubePerRole,
      viewFiltersPerCubePerRole,
      hasAllowAllForCube
    );
    if (rlsFilter) {
      query.filters = query.filters || [];
      query.filters.push(rlsFilter);
    }
    return { query, denied: false };
  }

  public removeEmptyFilters(filter: any): any {
    if (filter?.and) {
      const and = filter.and.map((f: any) => this.removeEmptyFilters(f)).filter((f: any) => f);
      return and.length > 1 ? { and } : and.at(0) || null;
    }
    if (filter?.or) {
      const or = filter.or.map((f: any) => this.removeEmptyFilters(f)).filter((f: any) => f);
      return or.length > 1 ? { or } : or.at(0) || null;
    }
    return filter;
  }

  public buildFinalRlsFilter(
    cubeFiltersPerCubePerRole: Record<string, Record<string, NestedFilter[]>>,
    viewFiltersPerCubePerRole: Record<string, Record<string, NestedFilter[]>>,
    hasAllowAllForCube: Record<string, boolean>
  ): any {
    const roleReducer = (filtersMap: Record<string, Record<string, NestedFilter[]>>) => (
      acc: Record<string, NestedFilter[]>,
      cubeName: string
    ): Record<string, NestedFilter[]> => {
      if (!hasAllowAllForCube[cubeName]) {
        Object.keys(filtersMap[cubeName]).forEach(role => {
          acc[role] = (acc[role] || []).concat(filtersMap[cubeName][role]);
        });
      }
      return acc;
    };

    const cubeFiltersPerRole = Object.keys(cubeFiltersPerCubePerRole).reduce(
      roleReducer(cubeFiltersPerCubePerRole),
      {}
    );
    const viewFiltersPerRole = Object.keys(viewFiltersPerCubePerRole).reduce(
      roleReducer(viewFiltersPerCubePerRole),
      {}
    );

    return this.removeEmptyFilters({
      and: [{
        or: Object.keys(cubeFiltersPerRole).map(role => ({
          and: cubeFiltersPerRole[role]
        }))
      }, {
        or: Object.keys(viewFiltersPerRole).map(role => ({
          and: viewFiltersPerRole[role]
        }))
      }]
    });
  }

  public async compilerCacheFn(
    requestId: string,
    key: any,
    path: string[]
  ): Promise<(subKey: string[], cacheFn: () => any) => any> {
    const compilers = await this.getCompilers({ requestId });
    if (this.sqlCache) {
      return (subKey: string[], cacheFn: () => any) => compilers.compilerCache.getQueryCache(key).cache(path.concat(subKey), cacheFn);
    } else {
      return (subKey: string[], cacheFn: () => any) => cacheFn();
    }
  }

  public async preAggregations(filter: any): Promise<any[]> {
    const { cubeEvaluator } = await this.getCompilers();
    return cubeEvaluator.preAggregations(filter);
  }

  public async scheduledPreAggregations(): Promise<any[]> {
    const { cubeEvaluator } = await this.getCompilers();
    return cubeEvaluator.scheduledPreAggregations();
  }

  public async createQueryByDataSource(
    compilers: CompilersResult,
    query: any,
    dataSource?: string,
    dbType?: DatabaseType
  ): Promise<any> {
    if (!dbType) {
      dbType = await this.getDbType(dataSource || 'default');
    }

    return this.createQuery(compilers, dbType, this.getDialectClass(dataSource || 'default', dbType), query);
  }

  public createQuery(
    compilers: CompilersResult,
    dbType: DatabaseType,
    dialectClass: any,
    query: any
  ): any {
    return createQuery(
      compilers,
      dbType,
      {
        ...query,
        dialectClass,
        externalDialectClass: this.options.externalDialectClass,
        externalDbType: this.options.externalDbType,
        preAggregationsSchema: this.preAggregationsSchema,
        allowUngroupedWithoutPrimaryKey: this.allowUngroupedWithoutPrimaryKey,
        convertTzForRawTimeDimension: this.convertTzForRawTimeDimension,
        queryFactory: this.queryFactory,
      }
    );
  }

  public async patchVisibilityByAccessPolicy(
    compilers: CompilersResult,
    context: RequestContext,
    cubes: CubeWithConfig[]
  ): Promise<{ cubes: CubeWithConfig[]; visibilityMaskHash: string | null }> {
    const isMemberVisibleInContext: Record<string, boolean> = {};
    const { cubeEvaluator } = compilers;

    if (!cubeEvaluator.isRbacEnabled()) {
      return { cubes, visibilityMaskHash: null };
    }

    for (const cube of cubes) {
      const evaluatedCube = cubeEvaluator.cubeFromPath(cube.config.name);
      if (cubeEvaluator.isRbacEnabledForCube(evaluatedCube)) {
        const applicablePolicies = await this.getApplicablePolicies(evaluatedCube, context, compilers);

        const computeMemberVisibility = (item: { name: string }): boolean => {
          for (const policy of applicablePolicies) {
            if (policy.memberLevel) {
              if (policy.memberLevel.includesMembers.includes(item.name) &&
               !policy.memberLevel.excludesMembers.includes(item.name)) {
                return true;
              }
            } else {
              return true;
            }
          }
          return false;
        };

        for (const dimension of cube.config.dimensions || []) {
          isMemberVisibleInContext[dimension.name] = computeMemberVisibility(dimension);
        }

        for (const measure of cube.config.measures || []) {
          isMemberVisibleInContext[measure.name] = computeMemberVisibility(measure);
        }

        for (const segment of cube.config.segments || []) {
          isMemberVisibleInContext[segment.name] = computeMemberVisibility(segment);
        }

        for (const hierarchy of cube.config.hierarchies || []) {
          isMemberVisibleInContext[hierarchy.name] = computeMemberVisibility(hierarchy);
        }
      }
    }

    const visibilityPatcherForCube = (cube: CubeWithConfig) => {
      const evaluatedCube = cubeEvaluator.cubeFromPath(cube.config.name);
      if (!cubeEvaluator.isRbacEnabledForCube(evaluatedCube)) {
        return (item: any) => item;
      }
      return (item: any) => ({
        ...item,
        isVisible: item.isVisible && isMemberVisibleInContext[item.name],
        public: item.public && isMemberVisibleInContext[item.name]
      });
    };

    const visibiliyMask = JSON.stringify(isMemberVisibleInContext, Object.keys(isMemberVisibleInContext).sort());
    const visibilityMaskHash = crypto.createHash('sha256').update(visibiliyMask).digest('hex');

    return {
      cubes: cubes
        .map((cube) => ({
          config: {
            ...cube.config,
            measures: cube.config.measures?.map(visibilityPatcherForCube(cube)),
            dimensions: cube.config.dimensions?.map(visibilityPatcherForCube(cube)),
            segments: cube.config.segments?.map(visibilityPatcherForCube(cube)),
            hierarchies: cube.config.hierarchies?.map(visibilityPatcherForCube(cube)),
          },
        })),
      visibilityMaskHash
    };
  }

  public mixInVisibilityMaskHash(compilerId: string, visibilityMaskHash: string): string {
    const uuidBytes = uuidParse(compilerId);
    const hashBytes = Buffer.from(visibilityMaskHash, 'hex');
    return uuidv4({
      random: crypto.createHash('sha256')
        .update(uuidBytes)
        .update(hashBytes)
        .digest()
        .subarray(0, 16) as Uint8Array
    });
  }

  public async metaConfig(
    requestContext: RequestContext,
    options: { includeCompilerId?: boolean; requestId?: string } = {}
  ): Promise<any> {
    const { includeCompilerId, ...restOptions } = options;
    const compilers = await this.getCompilers(restOptions);
    const { cubes } = compilers.metaTransformer;
    const { visibilityMaskHash, cubes: patchedCubes } = await this.patchVisibilityByAccessPolicy(
      compilers,
      requestContext,
      cubes
    );
    if (includeCompilerId) {
      return {
        cubes: patchedCubes,
        compilerId: visibilityMaskHash ?
          this.mixInVisibilityMaskHash(compilers.compilerId, visibilityMaskHash) :
          compilers.compilerId,
      };
    }
    return patchedCubes;
  }

  public async metaConfigExtended(
    requestContext: RequestContext,
    options: { requestId?: string }
  ): Promise<{ metaConfig: CubeWithConfig[]; cubeDefinitions: any }> {
    const compilers = await this.getCompilers(options);
    const { cubes: patchedCubes } = await this.patchVisibilityByAccessPolicy(
      compilers,
      requestContext,
      compilers.metaTransformer?.cubes
    );
    return {
      metaConfig: patchedCubes,
      cubeDefinitions: compilers.metaTransformer?.cubeEvaluator?.cubeDefinitions,
    };
  }

  public async compilerId(options: { requestId?: string } = {}): Promise<string> {
    return (await this.getCompilers(options)).compilerId;
  }

  public async cubeNameToDataSource(query: { requestId?: string }): Promise<Record<string, string>> {
    const { cubeEvaluator } = await this.getCompilers({ requestId: query.requestId });
    return cubeEvaluator
      .cubeNames()
      .map(
        (cube) => ({ [cube]: cubeEvaluator.cubeFromPath(cube).dataSource || 'default' })
      ).reduce((a, b) => ({ ...a, ...b }), {});
  }

  public async memberToDataSource(query: { requestId?: string }): Promise<Record<string, string>> {
    const { cubeEvaluator } = await this.getCompilers({ requestId: query.requestId });

    const entries = cubeEvaluator
      .cubeNames()
      .flatMap(cube => {
        const cubeDef = cubeEvaluator.cubeFromPath(cube);
        if (cubeDef.isView) {
          const viewName = cubeDef.name;
          return cubeDef.includedMembers.map((included: any) => {
            const memberName = `${viewName}.${included.name}`;
            const refCubeDef = cubeEvaluator.cubeFromPath(included.memberPath);
            const dataSource = refCubeDef.dataSource ?? 'default';
            return [memberName, dataSource];
          });
        } else {
          const cubeName = cubeDef.name;
          const dataSource = cubeDef.dataSource ?? 'default';
          return [
            ...Object.keys(cubeDef.dimensions),
            ...Object.keys(cubeDef.measures),
            ...Object.keys(cubeDef.segments),
          ].map(mem => [`${cubeName}.${mem}`, dataSource]);
        }
      });
    return Object.fromEntries(entries);
  }

  public async dataSources(
    orchestratorApi: { driverFactory: (dataSource: string) => Promise<BaseDriver> },
    query?: { requestId?: string }
  ): Promise<{ dataSources: Array<{ dataSource: string; dbType: DatabaseType }> }> {
    const cubeNameToDataSource = await this.cubeNameToDataSource(query || { requestId: `datasources-${uuidv4()}` });

    let dataSources = Object.keys(cubeNameToDataSource).map(c => cubeNameToDataSource[c]);

    dataSources = [...new Set(dataSources)];

    const dataSourcesWithTypes = await Promise.all(
      dataSources.map(async (dataSource) => {
        try {
          await orchestratorApi.driverFactory(dataSource);
          const dbType = await this.getDbType(dataSource);
          return { dataSource, dbType };
        } catch (err) {
          return null;
        }
      })
    );

    return {
      dataSources: dataSourcesWithTypes.filter((source): source is { dataSource: string; dbType: DatabaseType } => source !== null),
    };
  }

  public canUsePreAggregationForTransformedQuery(transformedQuery: any, refs: any): any {
    return PreAggregations.canUsePreAggregationForTransformedQueryFn(transformedQuery, refs);
  }
}
