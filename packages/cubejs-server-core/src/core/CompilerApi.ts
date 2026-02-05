import crypto from 'crypto';
import vm from 'vm';
import {
  AccessPolicyDefinition,
  BaseQuery,
  CanUsePreAggregationFn,
  compile,
  Compiler,
  createQuery,
  CubeDefinition,
  EvaluatedCube,
  PreAggregationFilters,
  PreAggregationInfo,
  PreAggregationReferences,
  PreAggregations,
  prepareCompiler,
  queryClass,
  QueryFactory,
  TransformedQuery,
  ViewIncludedMember,
} from '@cubejs-backend/schema-compiler';
import { GraphQLSchema } from 'graphql';
import { parse as uuidParse, v4 as uuidv4 } from 'uuid';
import { LRUCache } from 'lru-cache';
import { NativeInstance } from '@cubejs-backend/native';
import { disposedProxy, defaultHasher } from '@cubejs-backend/shared';
import type { SchemaFileRepository } from '@cubejs-backend/shared';
import { NormalizedQuery, MemberExpression } from '@cubejs-backend/api-gateway';
import { DriverCapabilities } from '@cubejs-backend/base-driver';
import { DbTypeInternalFn, DialectClassFn, LoggerFn } from './types';

type Context = any;

export interface CompilerApiOptions {
  dialectClass?: DialectClassFn;
  logger?: LoggerFn;
  preAggregationsSchema?: string | ((context: Context) => string | Promise<string>);
  allowUngroupedWithoutPrimaryKey?: boolean;
  convertTzForRawTimeDimension?: boolean;
  schemaVersion?: () => string | object | Promise<string | object>;
  contextToRoles?: (context: Context) => string[] | Promise<string[]>;
  contextToGroups?: (context: Context) => string[] | Promise<string[]>;
  compileContext?: any;
  allowJsDuplicatePropsInSchema?: boolean;
  sqlCache?: boolean;
  standalone?: boolean;
  compilerCacheSize?: number;
  maxCompilerCacheKeepAlive?: number;
  updateCompilerCacheKeepAlive?: boolean;
  externalDialectClass?: BaseQuery;
  externalDbType?: string;
  devServer?: boolean;
  fastReload?: boolean;
  allowNodeRequire?: boolean;
}

export interface GetSqlOptions {
  includeDebugInfo?: boolean;
  exportAnnotatedSql?: boolean;
  requestId?: string;
}

export interface SqlResult {
  external: any;
  sql: any;
  lambdaQueries: any;
  timeDimensionAlias?: string;
  timeDimensionField?: string;
  order?: any;
  cacheKeyQueries: any;
  preAggregations: any;
  dataSource: string;
  aliasNameToMember: any;
  rollupMatchResults?: any;
  canUseTransformedQuery: boolean;
  memberNames: string[];
}

export interface DataSourceInfo {
  dataSource: string;
  dbType: string;
  driverCapabilities?: DriverCapabilities;
}

export class CompilerApi {
  protected readonly repository: SchemaFileRepository;

  protected readonly dbType: DbTypeInternalFn;

  protected readonly dialectClass?: DialectClassFn;

  public readonly options: CompilerApiOptions;

  protected readonly allowNodeRequire: boolean;

  protected readonly logger: (msg: string, params: any) => void;

  protected readonly preAggregationsSchema?: string | ((context: Context) => string | Promise<string>);

  protected readonly allowUngroupedWithoutPrimaryKey?: boolean;

  protected readonly convertTzForRawTimeDimension?: boolean;

  public schemaVersion?: () => string | object | Promise<string | object>;

  protected readonly contextToRoles?: (context: Context) => string[] | Promise<string[]>;

  protected readonly contextToGroups?: (context: Context) => string[] | Promise<string[]>;

  protected readonly compileContext?: any;

  protected readonly allowJsDuplicatePropsInSchema?: boolean;

  protected readonly sqlCache?: boolean;

  protected readonly standalone?: boolean;

  protected readonly nativeInstance: NativeInstance;

  protected readonly compiledScriptCache: LRUCache<string, vm.Script>;

  protected readonly compiledYamlCache: LRUCache<string, string>;

  protected readonly compiledJinjaCache: LRUCache<string, string>;

  protected compiledScriptCacheInterval?: NodeJS.Timeout;

  protected graphqlSchema?: GraphQLSchema;

  protected compilers?: Promise<Compiler>;

  protected compilerVersion?: string;

  protected queryFactory?: QueryFactory;

  public constructor(repository: SchemaFileRepository, dbType: DbTypeInternalFn, options: CompilerApiOptions) {
    this.repository = repository;
    this.dbType = dbType;
    this.dialectClass = options.dialectClass;
    this.options = options || {};
    this.allowNodeRequire = options.allowNodeRequire == null ? true : options.allowNodeRequire;
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    this.logger = this.options.logger || (() => {});
    this.preAggregationsSchema = this.options.preAggregationsSchema;
    this.allowUngroupedWithoutPrimaryKey = this.options.allowUngroupedWithoutPrimaryKey;
    this.convertTzForRawTimeDimension = this.options.convertTzForRawTimeDimension;
    this.schemaVersion = this.options.schemaVersion;
    this.contextToRoles = this.options.contextToRoles;
    this.contextToGroups = this.options.contextToGroups;
    this.compileContext = options.compileContext;
    this.allowJsDuplicatePropsInSchema = options.allowJsDuplicatePropsInSchema;
    this.sqlCache = options.sqlCache;
    this.standalone = options.standalone;
    this.nativeInstance = this.createNativeInstance();

    // Caching stuff
    this.compiledScriptCache = new LRUCache({
      max: options.compilerCacheSize || 250,
      ttl: options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: options.updateCompilerCacheKeepAlive
    });
    this.compiledYamlCache = new LRUCache({
      max: options.compilerCacheSize || 250,
      ttl: options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: options.updateCompilerCacheKeepAlive
    });
    this.compiledJinjaCache = new LRUCache({
      max: options.compilerCacheSize || 250,
      ttl: options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: options.updateCompilerCacheKeepAlive
    });

    // proactively free up old cache values occasionally
    if (this.options.maxCompilerCacheKeepAlive) {
      this.compiledScriptCacheInterval = setInterval(
        () => {
          this.compiledScriptCache.purgeStale();
          this.compiledYamlCache.purgeStale();
          this.compiledJinjaCache.purgeStale();
        },
        this.options.maxCompilerCacheKeepAlive
      );
    }
  }

  public dispose(): void {
    if (this.compiledScriptCacheInterval) {
      clearInterval(this.compiledScriptCacheInterval);
      this.compiledScriptCacheInterval = null;
    }

    // freeing memory-heavy allocated instances
    // using safeguard for potential dangling references.
    this.compilers = disposedProxy('compilers', 'disposed CompilerApi instance');
    this.queryFactory = disposedProxy('queryFactory', 'disposed CompilerApi instance');
    this.graphqlSchema = undefined;
  }

  public setGraphQLSchema(schema: GraphQLSchema): void {
    this.graphqlSchema = schema;
  }

  public getGraphQLSchema(): GraphQLSchema {
    return this.graphqlSchema;
  }

  public createNativeInstance(): NativeInstance {
    return new NativeInstance();
  }

  public async getCompilers(options: { requestId?: string } = {}): Promise<Compiler> {
    let compilerVersion = (
      this.schemaVersion && await this.schemaVersion() ||
      'default_schema_version'
    );

    if (typeof compilerVersion === 'object') {
      compilerVersion = JSON.stringify(compilerVersion);
    }

    if (this.options.devServer || this.options.fastReload) {
      const files = await this.repository.dataSchemaFiles();
      compilerVersion += `_${defaultHasher().update(JSON.stringify(files)).digest('hex')}`;
    }

    if (!this.compilers || this.compilerVersion !== compilerVersion) {
      this.compilers = this.compileSchema(compilerVersion, options.requestId).catch(e => {
        this.compilers = undefined;
        throw e;
      });
      this.compilerVersion = compilerVersion;
    }

    return this.compilers;
  }

  /**
   * Creates the compilers instances without model compilation,
   * because it could fail and no compilers will be returned.
   */
  public createCompilerInstances(): Compiler {
    return prepareCompiler(this.repository, {
      allowNodeRequire: this.allowNodeRequire,
      compileContext: this.compileContext,
      allowJsDuplicatePropsInSchema: this.allowJsDuplicatePropsInSchema,
      standalone: this.standalone,
      nativeInstance: this.nativeInstance,
      compiledScriptCache: this.compiledScriptCache,
    });
  }

  public async compileSchema(compilerVersion: string, requestId?: string): Promise<Compiler> {
    const startCompilingTime = new Date().getTime();
    try {
      this.logger(this.compilers ? 'Recompiling schema' : 'Compiling schema', {
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
        compiledJinjaCache: this.compiledJinjaCache,
        compiledYamlCache: this.compiledYamlCache,
      });
      this.queryFactory = await this.createQueryFactory(compilers);

      this.logger('Compiling schema completed', {
        version: compilerVersion,
        requestId,
        duration: ((new Date()).getTime() - startCompilingTime),
      });

      return compilers;
    } catch (e: any) {
      this.logger('Compiling schema error', {
        version: compilerVersion,
        requestId,
        duration: ((new Date()).getTime() - startCompilingTime),
        error: (e.stack || e).toString()
      });
      throw e;
    }
  }

  public async createQueryFactory(compilers: Compiler): Promise<QueryFactory> {
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

  public async getDbType(dataSource: string = 'default'): Promise<string> {
    return this.dbType({ dataSource });
  }

  public getDialectClass(dataSource: string = 'default', dbType: string): BaseQuery {
    return this.dialectClass?.({ dataSource, dbType });
  }

  public async getSqlGenerator(query: NormalizedQuery, dataSource?: string): Promise<{ sqlGenerator: any; compilers: Compiler }> {
    const dbType = await this.getDbType(dataSource);
    const compilers = await this.getCompilers({ requestId: query.requestId });
    let sqlGenerator = await this.createQueryByDataSource(compilers, query, dataSource, dbType);

    if (!sqlGenerator) {
      throw new Error(`Unknown dbType: ${dbType}`);
    }

    // sqlGenerator.dataSource can return undefined for query without members
    // Queries like this are used by api-gateway to initialize SQL API
    // At the same time, those queries should use concrete dataSource, so we should be good to go with it
    dataSource = compilers.compiler.withQuery(sqlGenerator, () => sqlGenerator.dataSource);
    if (dataSource !== undefined) {
      const _dbType = await this.getDbType(dataSource);
      if (dataSource !== 'default' && dbType !== _dbType) {
        // TODO consider more efficient way than instantiating query
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

  public async getSql(query: NormalizedQuery, options: GetSqlOptions = {}): Promise<SqlResult> {
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
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { requestId, ...keyOptions } = query;
      const key = { query: keyOptions, options };
      return compilers.compilerCache.getQueryCache(key).cache(['sql'], getSqlFn);
    } else {
      return getSqlFn();
    }
  }

  protected async getRolesFromContext(context: Context): Promise<Set<string>> {
    if (!this.contextToRoles) {
      return new Set();
    }
    return new Set(await this.contextToRoles(context));
  }

  protected async getGroupsFromContext(context: Context): Promise<Set<string>> {
    if (!this.contextToGroups) {
      return new Set();
    }
    return new Set(await this.contextToGroups(context));
  }

  protected userHasRole(userRoles: Set<string>, role: string): boolean {
    return userRoles.has(role) || role === '*';
  }

  protected userHasGroup(userGroups: Set<string>, group: string | string[]): boolean {
    if (Array.isArray(group)) {
      return group.some(g => userGroups.has(g) || g === '*');
    }
    return userGroups.has(group) || group === '*';
  }

  protected roleMeetsConditions(evaluatedConditions?: any[]): boolean {
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

  protected async getCubesFromQuery(query: NormalizedQuery, context?: Context): Promise<Set<string>> {
    const sql = await this.getSql(query, { requestId: context?.requestId });
    return new Set(sql.memberNames.map(memberName => memberName.split('.')[0]));
  }

  protected hashRequestContext(context: Context): string {
    if (!context.__hash) {
      context.__hash = defaultHasher().update(JSON.stringify(context)).digest('hex');
    }
    return context.__hash;
  }

  protected async getApplicablePolicies(cube: EvaluatedCube, context: Context, compilers: Compiler): Promise<any[]> {
    const cache = compilers.compilerCache.getRbacCacheInstance();
    const cacheKey = `${cube.name}_${this.hashRequestContext(context)}`;
    if (!cache.has(cacheKey)) {
      const userRoles = await this.getRolesFromContext(context);
      const userGroups = await this.getGroupsFromContext(context);
      const policies = cube.accessPolicy.filter((policy: AccessPolicyDefinition) => {
        // Validate that policy doesn't have both role and group/groups - this is invalid
        if (policy.role && (policy.group || policy.groups)) {
          const groupValue = policy.group || policy.groups;
          const groupDisplay = Array.isArray(groupValue) ? groupValue.join(', ') : groupValue;
          const groupProp = policy.group ? 'group' : 'groups';
          throw new Error(
            `Access policy cannot have both 'role' and '${groupProp}' properties.\nPolicy in cube '${cube.name}' has role '${policy.role}' and ${groupProp} '${groupDisplay}'.\nUse either 'role' or '${groupProp}', not both.`
          );
        }

        // Validate that policy doesn't have both group and groups
        if (policy.group && policy.groups) {
          const groupDisplay = Array.isArray(policy.group) ? policy.group.join(', ') : policy.group;
          const groupsDisplay = Array.isArray(policy.groups) ? policy.groups.join(', ') : policy.groups;
          throw new Error(
            `Access policy cannot have both 'group' and 'groups' properties.\nPolicy in cube '${cube.name}' has group '${groupDisplay}' and groups '${groupsDisplay}'.\nUse either 'group' or 'groups', not both.`
          );
        }

        const evaluatedConditions = (policy.conditions || []).map(
          (condition: any) => compilers.cubeEvaluator.evaluateContextFunction(cube, condition.if, context)
        );

        // Check if policy matches by role, group, or groups
        let hasAccess = false;

        if (policy.role) {
          hasAccess = this.userHasRole(userRoles, policy.role);
        } else if (policy.group) {
          hasAccess = this.userHasGroup(userGroups, policy.group);
        } else if (policy.groups) {
          hasAccess = this.userHasGroup(userGroups, policy.groups);
        } else {
          // If policy has neither role nor group/groups, default to checking role for backward compatibility
          hasAccess = this.userHasRole(userRoles, '*');
        }

        const res = hasAccess && this.roleMeetsConditions(evaluatedConditions);
        return res;
      });
      cache.set(cacheKey, policies);
    }
    return cache.get(cacheKey);
  }

  protected evaluateNestedFilter(filter: any, cube: any, context: Context, cubeEvaluator: any): any {
    const result: any = {};
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

  /**
   * This method rewrites the query according to RBAC row level security policies.
   *
   * If RBAC is enabled, it looks at all the Cubes from the query with accessPolicy defined.
   * It extracts all policies applicable to for the current user context (contextToRoles() + conditions).
   * It then generates a rls filter by
   * - combining all filters for the same role with AND
   * - combining all filters for different roles with OR
   * - combining cube and view filters with AND
  */
  public async applyRowLevelSecurity(
    query: NormalizedQuery,
    evaluatedQuery: NormalizedQuery,
    context: Context
  ): Promise<{ query: NormalizedQuery; denied: boolean }> {
    const compilers = await this.getCompilers({ requestId: context.requestId });
    const { cubeEvaluator } = compilers;

    if (!cubeEvaluator.isRbacEnabled()) {
      return { query, denied: false };
    }

    // Get the SQL to extract member names from the query
    const sql = await this.getSql(evaluatedQuery, { requestId: context?.requestId });
    const queryMemberNames = new Set(sql.memberNames);
    const queryCubes = new Set(sql.memberNames.map(memberName => memberName.split('.')[0]));

    // Identify cubes that are accessed through views.
    // Similar to PostgreSQL views: views act as a security boundary for member access.
    // When a cube is accessed via a view, we skip the cube's member-level restrictions
    // and only apply row-level filters. The view controls what members are exposed.
    const cubesAccessedViaView = new Set<string>();
    for (const cubeName of queryCubes) {
      const cube = cubeEvaluator.cubeFromPath(cubeName);
      if (cube.isView) {
        // Track which underlying cubes are accessed through this view
        const underlyingCubes = new Set(
          (cube.includedMembers || []).map((m: any) => m.memberPath.split('.')[0])
        );
        underlyingCubes.forEach(c => cubesAccessedViaView.add(c));
      }
    }

    // We collect Cube and View filters separately because they have to be
    // applied in "two layers": first Cube filters, then View filters on top
    const cubeFiltersPerCubePerRole: Record<string, Record<string, any[]>> = {};
    const viewFiltersPerCubePerRole: Record<string, Record<string, any[]>> = {};
    const hasAllowAllForCube: Record<string, boolean> = {};

    for (const cubeName of queryCubes) {
      const cube = cubeEvaluator.cubeFromPath(cubeName);
      const filtersMap = cube.isView ? viewFiltersPerCubePerRole : cubeFiltersPerCubePerRole;

      if (cubeEvaluator.isRbacEnabledForCube(cube)) {
        let hasAccessPermission = false;
        const userPolicies = await this.getApplicablePolicies(cube, context, compilers);

        // Filter out policies that don't grant member-level access to query members
        //
        // Policies define access in two dimensions: Members (columns) and Rows.
        // We first filter by member access, then apply row-level filters.
        //
        // Example setup:
        //   - Policy 1 covers members: a, b (with row filter R1)
        //   - Policy 2 covers members: b, c (with row filter R2)
        //
        //   Members
        //     ^
        //     |       ┌─────────────────────────────┐
        //   c |       │          Policy 2           │
        //     |   ┌───┼─────────────┐               │
        //   b |   │   │  (overlap)  │               │
        //     |   │   └─────────────┼───────────────┘
        //   a |   │    Policy 1     │
        //     |   └─────────────────┘
        //     └──────────────────────────────────────────> Rows
        //              R1 rows        R2 rows
        //
        // ═══════════════════════════════════════════════════════════════════
        // Case 1: Query members (a, b)
        //         Only Policy 1 covers ALL queried members → R1 rows visible
        //
        //   Members
        //     ^
        //     |       ┌─────────────────────────────┐
        //   c |       │          Policy 2           │
        //     |   ┌───┼─────────────┐               │
        //   b |   │░░░│░░(query)░░░░│               │
        //     |   │░░░└─────────────┼───────────────┘
        //   a |   │░░░░Policy 1░░░░░│
        //     |   └─────────────────┘
        //     └──────────────────────────────────────────> Rows
        //         ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
        //         R1 rows visible
        //
        // ═══════════════════════════════════════════════════════════════════
        // Case 2: Query members (b, c)
        //         Only Policy 2 covers ALL queried members → R2 rows visible
        //
        //   Members
        //     ^
        //     |       ┌─────────────────────────────┐
        //   c |       │░░░░░░░░░░Policy 2░░░░░░░░░░░│
        //     |   ┌───┼─────────────┐░░░░░░░░░░░░░░░│
        //   b |   │   │░░(query)░░░░│░░░░░░░░░░░░░░░│
        //     |   │   └─────────────┼───────────────┘
        //   a |   │    Policy 1     │
        //     |   └─────────────────┘
        //     └──────────────────────────────────────────> Rows
        //             ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
        //             R2 rows visible
        //
        // ═══════════════════════════════════════════════════════════════════
        // Case 3: Query member (b) only
        //         Both policies cover member b → Union of R1 ∪ R2 rows visible
        //
        //   Members
        //     ^
        //     |       ┌─────────────────────────────┐
        //   c |       │          Policy 2           │
        //     |   ┌───┼─────────────┐               │
        //   b |   │░░░│░░(query)░░░░│░░░░░░░░░░░░░░░│
        //     |   │   └─────────────┼───────────────┘
        //   a |   │    Policy 1     │
        //     |   └─────────────────┘
        //     └──────────────────────────────────────────> Rows
        //         ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
        //         R1 ∪ R2 rows visible (union)
        //
        // ═══════════════════════════════════════════════════════════════════
        // Case 4: Query members (a, b, c)
        //         Neither policy covers ALL three → NO rows visible (denied)
        //
        //   Members
        //     ^
        //     |       ┌─────────────────────────────┐
        //   c |       │          Policy 2           │
        //     |   ┌───┼─────────────┐               │
        //   b |   │   │  (query)    │               │
        //     |   │   └─────────────┼───────────────┘
        //   a |   │    Policy 1     │
        //     |   └─────────────────┘
        //     └──────────────────────────────────────────> Rows
        //
        //         No policy covers {a,b,c} → Access denied, empty result
        //
        const policiesWithMemberAccess = userPolicies.filter((policy: any) => {
          // If there's no memberLevel policy, all members are accessible
          if (!policy.memberLevel) {
            return true;
          }

          // PostgreSQL-style view behavior: if this cube is accessed through a view,
          // the view grants access to all members it exposes.
          // We only apply row-level filters from the cube, not member-level restrictions.
          if (cubesAccessedViaView.has(cubeName)) {
            return true;
          }

          const cubeMembersInQuery = Array.from(queryMemberNames).filter(
            memberName => memberName.startsWith(`${cubeName}.`)
          );

          // Check if the policy grants access to all members used in the query
          return [...cubeMembersInQuery].every(memberName => policy.memberLevel.includesMembers.includes(memberName) &&
            !policy.memberLevel.excludesMembers.includes(memberName));
        });

        for (const policy of policiesWithMemberAccess) {
          hasAccessPermission = true;
          (policy?.rowLevel?.filters || []).forEach((filter: any) => {
            filtersMap[cubeName] = filtersMap[cubeName] || {};
            // Create a unique key for the policy (either role, group, or groups)
            const groupValue = policy.group || policy.groups;
            const policyKey = policy.role ||
              (Array.isArray(groupValue) ? groupValue.join(',') : groupValue) ||
              'default';
            filtersMap[cubeName][policyKey] = filtersMap[cubeName][policyKey] || [];
            filtersMap[cubeName][policyKey].push(
              this.evaluateNestedFilter(filter, cube, context, cubeEvaluator)
            );
          });
          if (!policy?.rowLevel || policy?.rowLevel?.allowAll) {
            hasAllowAllForCube[cubeName] = true;
            // We don't have a way to add an "all allowed" filter like `WHERE 1 = 1` or something.
            // Instead, we'll just mark that the user has "all" access to a given cube and remove
            // all filters later
            break;
          }
        }

        if (!hasAccessPermission) {
          // This is a hack that will make sure that the query returns no result
          query.segments = query.segments || [];
          query.segments.push({
            expression: () => '1 = 0',
            cubeName: cube.name,
            name: 'rlsAccessDenied',
          } as unknown as MemberExpression);
          // If we hit this condition there's no need to evaluate the rest of the policy
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

  protected removeEmptyFilters(filter: any): any {
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

  protected buildFinalRlsFilter(
    cubeFiltersPerCubePerRole: Record<string, Record<string, any[]>>,
    viewFiltersPerCubePerRole: Record<string, Record<string, any[]>>,
    hasAllowAllForCube: Record<string, boolean>
  ): any {
    // - delete all filters for cubes where the user has allowAll
    // - combine the rest into per policy maps (policies can be role-based or group-based)
    // - join all filters for the same policy with AND
    // - join all filters for different policies with OR
    // - join cube and view filters with AND

    const policyReducer = (filtersMap: Record<string, Record<string, any[]>>) => (acc: Record<string, any[]>, cubeName: string) => {
      if (!hasAllowAllForCube[cubeName]) {
        Object.keys(filtersMap[cubeName]).forEach(policyKey => {
          acc[policyKey] = (acc[policyKey] || []).concat(filtersMap[cubeName][policyKey]);
        });
      }
      return acc;
    };

    const cubeFiltersPerPolicy = Object.keys(cubeFiltersPerCubePerRole).reduce(
      policyReducer(cubeFiltersPerCubePerRole),
      {}
    );
    const viewFiltersPerPolicy = Object.keys(viewFiltersPerCubePerRole).reduce(
      policyReducer(viewFiltersPerCubePerRole),
      {}
    );

    return this.removeEmptyFilters({
      and: [{
        or: Object.keys(cubeFiltersPerPolicy).map(policyKey => ({
          and: cubeFiltersPerPolicy[policyKey]
        }))
      }, {
        or: Object.keys(viewFiltersPerPolicy).map(policyKey => ({
          and: viewFiltersPerPolicy[policyKey]
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
      return (subKey, cacheFn) => compilers.compilerCache.getQueryCache(key).cache(path.concat(subKey), cacheFn);
    } else {
      return (subKey, cacheFn) => cacheFn();
    }
  }

  public async preAggregations(filter?: PreAggregationFilters): Promise<PreAggregationInfo[]> {
    const { cubeEvaluator } = await this.getCompilers();
    return cubeEvaluator.preAggregations(filter);
  }

  public async scheduledPreAggregations(): Promise<PreAggregationInfo[]> {
    const { cubeEvaluator } = await this.getCompilers();
    return cubeEvaluator.scheduledPreAggregations();
  }

  public async createQueryByDataSource(
    compilers: Compiler,
    query: NormalizedQuery | {},
    dataSource?: string,
    dbType?: string
  ): Promise<BaseQuery> {
    if (!dbType) {
      dbType = await this.getDbType(dataSource);
    }

    return this.createQuery(compilers, dbType, this.getDialectClass(dataSource, dbType), query);
  }

  public createQuery(compilers: Compiler, dbType: string, dialectClass: BaseQuery, query: NormalizedQuery | {}): BaseQuery {
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

  /**
   * if RBAC is enabled, this method is used to patch isVisible property of cube members
   * based on access policies.
  */
  protected async patchVisibilityByAccessPolicy(
    compilers: Compiler,
    context: Context,
    cubes: any[]
  ): Promise<{ cubes: any[]; visibilityMaskHash: string | null }> {
    const isMemberVisibleInContext: Record<string, boolean> = {};
    const { cubeEvaluator } = compilers;

    if (!cubeEvaluator.isRbacEnabled()) {
      return { cubes, visibilityMaskHash: null };
    }

    for (const cube of cubes) {
      const evaluatedCube = cubeEvaluator.cubeFromPath(cube.config.name);
      if (cubeEvaluator.isRbacEnabledForCube(evaluatedCube)) {
        const applicablePolicies = await this.getApplicablePolicies(evaluatedCube, context, compilers);

        const computeMemberVisibility = (item: any): boolean => {
          for (const policy of applicablePolicies) {
            if (policy.memberLevel) {
              if (policy.memberLevel.includesMembers.includes(item.name) &&
               !policy.memberLevel.excludesMembers.includes(item.name)) {
                return true;
              }
            } else {
              // If there's no memberLevel policy, we assume that all members are visible
              return true;
            }
          }
          return false;
        };

        for (const dimension of cube.config.dimensions) {
          isMemberVisibleInContext[dimension.name] = computeMemberVisibility(dimension);
        }

        for (const measure of cube.config.measures) {
          isMemberVisibleInContext[measure.name] = computeMemberVisibility(measure);
        }

        for (const segment of cube.config.segments) {
          isMemberVisibleInContext[segment.name] = computeMemberVisibility(segment);
        }

        for (const hierarchy of cube.config.hierarchies) {
          isMemberVisibleInContext[hierarchy.name] = computeMemberVisibility(hierarchy);
        }
      }
    }

    const visibilityPatcherForCube = (cube: any) => {
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

    const visibilityMask = JSON.stringify(isMemberVisibleInContext, Object.keys(isMemberVisibleInContext).sort());
    // This hash will be returned along the modified meta config and can be used
    // to distinguish between different "schema versions" after DAP visibility is applied
    const visibilityMaskHash = crypto.createHash('sha256').update(visibilityMask).digest('hex');

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

  protected mixInVisibilityMaskHash(compilerId: string, visibilityMaskHash: string): string {
    const uuidBytes = Buffer.from(uuidParse(compilerId));
    const hashBytes = Buffer.from(visibilityMaskHash, 'hex');
    return uuidv4({ random: crypto.createHash('sha256').update(uuidBytes).update(hashBytes).digest()
      .subarray(0, 16) as any });
  }

  public async metaConfig(
    requestContext: Context,
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
        // This compilerId is primarily used by the cubejs-backend-native or caching purposes.
        // By default, it doesn't account for member visibility changes introduced above by DAP.
        // Here we're modifying the original compilerId in a way that it's distinct for
        // distinct schema versions while still being a valid UUID.
        compilerId: visibilityMaskHash ? this.mixInVisibilityMaskHash(compilers.compilerId, visibilityMaskHash) : compilers.compilerId,
      };
    }
    return patchedCubes;
  }

  public async metaConfigExtended(
    requestContext: Context,
    options?: { requestId?: string }
  ): Promise<{ metaConfig: any; cubeDefinitions: Record<string, CubeDefinition> }> {
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

  public async memberToDataSource(query: NormalizedQuery): Promise<Record<string, string>> {
    const { cubeEvaluator } = await this.getCompilers({ requestId: query.requestId });

    const entries = cubeEvaluator
      .cubeNames()
      .flatMap(cube => {
        const cubeDef = cubeEvaluator.cubeFromPath(cube);
        if (cubeDef.isView) {
          const viewName = cubeDef.name;
          return cubeDef.includedMembers?.map((included: ViewIncludedMember) => {
            const memberName = `${viewName}.${included.name}`;
            const refCubeDef = cubeEvaluator.cubeFromPath(included.memberPath);
            const dataSource = refCubeDef.dataSource ?? 'default';
            return [memberName, dataSource];
          }) || [];
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
    orchestratorApi: any,
    query?: NormalizedQuery,
    _dataSourceFromEnvs?: string[]
  ): Promise<{ dataSources: DataSourceInfo[] }> {
    const cubeNameToDataSource = await this.cubeNameToDataSource(query || { requestId: `datasources-${uuidv4()}` });

    let dataSources = Object.keys(cubeNameToDataSource).map(c => cubeNameToDataSource[c]);

    dataSources = [...new Set(dataSources)];

    const dataSourcesInfo = await Promise.all(
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
      dataSources: dataSourcesInfo.filter((source): source is DataSourceInfo => !!source),
    };
  }

  public canUsePreAggregationForTransformedQuery(transformedQuery: TransformedQuery, refs: PreAggregationReferences | null = null): CanUsePreAggregationFn {
    return PreAggregations.canUsePreAggregationForTransformedQueryFn(transformedQuery, refs);
  }
}
