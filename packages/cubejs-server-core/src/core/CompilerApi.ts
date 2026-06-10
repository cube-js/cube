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
import { disposedProxy } from '@cubejs-backend/shared';
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
      compilerVersion += `_${crypto.createHash('md5').update(JSON.stringify(files)).digest('hex')}`;
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

  protected async getGroupsFromContext(context: Context): Promise<Set<string>> {
    if (!this.contextToGroups) {
      return new Set();
    }
    return new Set(await this.contextToGroups(context));
  }

  protected userHasGroup(userGroups: Set<string>, group: string | string[]): boolean {
    if (Array.isArray(group)) {
      return group.some(g => userGroups.has(g) || g === '*');
    }
    return userGroups.has(group) || group === '*';
  }

  protected policyMeetsConditions(evaluatedConditions?: any[]): boolean {
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
      context.__hash = crypto.createHash('md5').update(JSON.stringify(context)).digest('hex');
    }
    return context.__hash;
  }

  protected async getApplicablePolicies(cube: EvaluatedCube, context: Context, compilers: Compiler): Promise<any[]> {
    const cache = compilers.compilerCache.getRbacCacheInstance();
    const cacheKey = `${cube.name}_${this.hashRequestContext(context)}`;
    if (!cache.has(cacheKey)) {
      const userGroups = await this.getGroupsFromContext(context);
      const policies = cube.accessPolicy.filter((policy: AccessPolicyDefinition) => {
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

        // Check if policy matches by group or groups
        let hasAccess = false;

        if (policy.group) {
          hasAccess = this.userHasGroup(userGroups, policy.group);
        } else if (policy.groups) {
          hasAccess = this.userHasGroup(userGroups, policy.groups);
        } else {
          // A policy without group/groups applies to everyone
          hasAccess = this.userHasGroup(userGroups, '*');
        }

        return hasAccess && this.policyMeetsConditions(evaluatedConditions);
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
   * It extracts all policies applicable to for the current user context (contextToGroups() + conditions).
   * It then generates a rls filter by
   * - combining all filters for the same group with AND
   * - combining all filters for different groups with OR
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

    // Per-cube row-level constraints, AND-ed together to form the final RLS
    // filter. Each cube/view contributes a single expression (see below).
    const rlsConstraints: any[] = [];
    const maskedMembersSet = new Set<string>();
    const memberMaskFiltersMap: Record<string, any> = {};

    for (const cubeName of queryCubes) {
      const cube = cubeEvaluator.cubeFromPath(cubeName);

      if (cubeEvaluator.isRbacEnabledForCube(cube)) {
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
        // Access is resolved PER MEMBER: a member is accessible if any policy
        // grants it (union), and visible rows are the intersection over queried
        // members of the union of each member's granting-policy row filters. An
        // allow-all (filter-less) policy imposes no row restriction.
        //
        // ═══════════════════════════════════════════════════════════════════
        // Case 1: Query members (a, b)
        //         a → R1, b → R1∪R2  ⇒  R1 ∩ (R1∪R2) = R1 rows visible
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
        //         b → R1∪R2, c → R2  ⇒  (R1∪R2) ∩ R2 = R2 rows visible
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
        //         b → R1∪R2  ⇒  R1 ∪ R2 rows visible (union)
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
        //         a → R1, b → R1∪R2, c → R2  ⇒  R1 ∩ R2 = ∅ → no rows visible
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
        //         Disjoint row ranges (R1 ∩ R2 = ∅) → empty result
        //
        const cubeMembersInQuery = Array.from(queryMemberNames).filter(
          memberName => memberName.startsWith(`${cubeName}.`)
        );

        // A policy "grants" a member if it exposes it through memberLevel (full
        // access) or memberMasking (masked access). Policies without memberLevel
        // and memberMasking expose everything, as do cubes reached through a view.
        const policyGrantsMember = (policy: any, memberName: string): boolean => {
          if (!policy.memberLevel && !policy.memberMasking) {
            return true;
          }
          if (cubesAccessedViaView.has(cubeName)) {
            return true;
          }
          const hasFullAccess = !policy.memberLevel ||
            (policy.memberLevel.includesMembers.includes(memberName) &&
             !policy.memberLevel.excludesMembers.includes(memberName));
          if (hasFullAccess) return true;

          if (policy.memberMasking) {
            return policy.memberMasking.includesMembers.includes(memberName) &&
                   !policy.memberMasking.excludesMembers.includes(memberName);
          }
          return false;
        };

        const policyRowFilter = (policy: any) => {
          const filters = (policy.rowLevel?.filters || []).map(
            (filter: any) => this.evaluateNestedFilter(filter, cube, context, cubeEvaluator)
          );
          return filters.length === 1 ? filters[0] : { and: filters };
        };

        const policyHasRowFilter = (policy: any): boolean => !!(
          policy.rowLevel && !policy.rowLevel.allowAll && policy.rowLevel.filters?.length > 0
        );

        // Access is resolved per member as the UNION across all matching policies:
        // a member is accessible if ANY applicable policy grants it (full or
        // masked). This is what makes multi-group access work — a user matching
        // several policies (e.g. via multiple groups) sees every member any of
        // those policies grants, even when no single policy covers all of them.
        //
        // Row-level access is resolved per member and then intersected: for each
        // queried member we take the OR of the row filters of the policies that
        // grant it (a policy without a row filter — allow-all — imposes no
        // restriction), and the cube's row constraint is the AND of those
        // per-member expressions. This keeps differently-filtered policies safe:
        //   - members covered by an allow-all policy impose no restriction;
        //   - querying members that live in disjoint row ranges intersects to no
        //     rows instead of leaking (the two-dimensional overlap case).
        //
        // Masking: a member is masked unless some granting policy provides
        // unconditional full access (memberLevel + allow-all). When the full
        // access is conditional on a row filter, that filter is recorded so the
        // SQL renders CASE WHEN {rowFilter} THEN {value} ELSE {mask} END.
        const memberRowConstraints: any[] = [];
        const seenRowConstraints = new Set<string>();
        let cubeAccessDenied = false;

        for (const memberName of cubeMembersInQuery) {
          const grantingPolicies = userPolicies.filter(
            (policy: any) => policyGrantsMember(policy, memberName)
          );

          if (grantingPolicies.length === 0) {
            cubeAccessDenied = true;
            break;
          }

          const hasUnconditionalFullAccess = grantingPolicies.some((policy: any) => {
            const inFullAccess = !policy.memberLevel ||
              (policy.memberLevel.includesMembers.includes(memberName) &&
               !policy.memberLevel.excludesMembers.includes(memberName));
            return inFullAccess && (!policy.rowLevel || policy.rowLevel.allowAll);
          });

          if (!hasUnconditionalFullAccess) {
            const hasMaskingPolicy = grantingPolicies.some(
              (policy: any) => policy.memberMasking &&
                policy.memberMasking.includesMembers.includes(memberName) &&
                !policy.memberMasking.excludesMembers.includes(memberName)
            );

            if (hasMaskingPolicy) {
              maskedMembersSet.add(memberName);

              const conditionalFullAccessPolicies = grantingPolicies.filter((policy: any) => {
                const hasFullMemberAccess = !policy.memberLevel ||
                  (policy.memberLevel.includesMembers.includes(memberName) &&
                   !policy.memberLevel.excludesMembers.includes(memberName));
                return hasFullMemberAccess && policyHasRowFilter(policy);
              });

              if (conditionalFullAccessPolicies.length > 0) {
                const policyFilters = conditionalFullAccessPolicies.map(policyRowFilter);
                memberMaskFiltersMap[memberName] = policyFilters.length === 1
                  ? policyFilters[0]
                  : { or: policyFilters };
              }
            }
          }

          // Row-level constraint for this member: OR of the row filters of every
          // granting policy. If any granting policy is allow-all the member is
          // visible on all rows, so it adds no restriction to the intersection.
          const memberHasUnrestrictedRow = grantingPolicies.some(
            (policy: any) => !policyHasRowFilter(policy)
          );
          if (!memberHasUnrestrictedRow) {
            const orClauses = grantingPolicies.map(policyRowFilter);
            const constraint = orClauses.length === 1 ? orClauses[0] : { or: orClauses };
            // Members granted by the same set of policies produce identical row
            // constraints; dedupe so the WHERE clause isn't repeated per member.
            const constraintKey = JSON.stringify(constraint);
            if (!seenRowConstraints.has(constraintKey)) {
              seenRowConstraints.add(constraintKey);
              memberRowConstraints.push(constraint);
            }
          }
        }

        if (cubeAccessDenied) {
          query.segments = query.segments || [];
          query.segments.push({
            expression: () => '1 = 0',
            cubeName: cube.name,
            name: 'rlsAccessDenied',
          } as unknown as MemberExpression);
          return { query, denied: true };
        }

        if (memberRowConstraints.length > 0) {
          rlsConstraints.push(
            memberRowConstraints.length === 1
              ? memberRowConstraints[0]
              : { and: memberRowConstraints }
          );
        }
      }
    }

    // If the query already constrains rows to a subset of a conditionally-masked
    // member's row filter, every returned row would satisfy that filter anyway —
    // so the conditional `CASE WHEN {rowFilter} THEN value ELSE mask END` is
    // redundant and the member can be unmasked. This is evaluated against the
    // user-supplied query filters (before the RLS filter is appended below). It
    // also lets a conditionally-masked aggregate measure render its real value
    // (instead of being fully masked) when the query is already scoped to the
    // filter's rows.
    for (const member of Array.from(maskedMembersSet)) {
      const maskFilter = memberMaskFiltersMap[member];
      if (maskFilter && this.queryFiltersImplyFilter(query.filters || [], maskFilter)) {
        maskedMembersSet.delete(member);
        delete memberMaskFiltersMap[member];
      }
    }

    const rlsFilter = rlsConstraints.length > 0
      ? this.removeEmptyFilters({ and: rlsConstraints })
      : null;
    if (rlsFilter) {
      query.filters = query.filters || [];
      query.filters.push(rlsFilter);
    }
    if (maskedMembersSet.size > 0) {
      query.maskedMembers = Array.from(maskedMembersSet).map(member => ({
        member,
        filter: memberMaskFiltersMap[member],
      }));
    }
    return { query, denied: false };
  }

  protected filterMemberName(filter: any): string | undefined {
    return filter?.member || filter?.dimension;
  }

  /**
   * Collects the leaf conditions that are guaranteed to hold for every row a
   * query returns — i.e. conditions that are AND-ed together at the top level.
   * OR groups weaken the guarantee, so their members are not collected.
   */
  protected collectGuaranteedFilterLeaves(filters: any[], acc: any[] = []): any[] {
    for (const filter of filters || []) {
      if (filter?.and) {
        this.collectGuaranteedFilterLeaves(filter.and, acc);
      } else if (filter && !filter.or && this.filterMemberName(filter) && filter.operator) {
        // OR groups (and falsy entries) don't guarantee a single branch, so skip them.
        acc.push(filter);
      }
    }
    return acc;
  }

  /**
   * Returns true when a query leaf condition is at least as restrictive as a
   * target leaf condition on the same member (i.e. the rows allowed by the query
   * condition are guaranteed to be a subset of the rows allowed by the target).
   * Intentionally conservative: when implication can't be proven soundly, returns
   * false so masking is preserved.
   */
  protected leafFilterImplies(queryLeaf: any, targetLeaf: any): boolean {
    if (this.filterMemberName(queryLeaf) !== this.filterMemberName(targetLeaf)) {
      return false;
    }
    const qOp = queryLeaf.operator;
    const tOp = targetLeaf.operator;
    const qVals = (queryLeaf.values || []).map((v: any) => String(v));
    const tVals = (targetLeaf.values || []).map((v: any) => String(v));

    // Identical condition.
    if (
      qOp === tOp &&
      qVals.length === tVals.length &&
      qVals.every((v: string) => tVals.includes(v)) &&
      tVals.every((v: string) => qVals.includes(v))
    ) {
      return true;
    }

    // equals/in: the query value set must be a subset of the target value set
    // (querying fewer values is more restrictive).
    if (qOp === 'equals' && tOp === 'equals') {
      return qVals.length > 0 && qVals.every((v: string) => tVals.includes(v));
    }

    // Numeric range bounds in the same direction.
    if (qVals.length === 1 && tVals.length === 1) {
      const q = Number(qVals[0]);
      const t = Number(tVals[0]);
      if (!Number.isNaN(q) && !Number.isNaN(t)) {
        // Lower bounds: x > / >= value.
        if ((qOp === 'gt' || qOp === 'gte') && (tOp === 'gt' || tOp === 'gte')) {
          // target `x > t`: a `gt q` query needs q >= t, a `gte q` query needs q > t.
          // target `x >= t`: q >= t for both query operators.
          if (tOp === 'gt') {
            return qOp === 'gt' ? q >= t : q > t;
          }
          return q >= t;
        }
        // Upper bounds: x < / <= value.
        if ((qOp === 'lt' || qOp === 'lte') && (tOp === 'lt' || tOp === 'lte')) {
          if (tOp === 'lt') {
            return qOp === 'lt' ? q <= t : q < t;
          }
          return q <= t;
        }
      }
    }

    return false;
  }

  protected filterConditionImplied(guaranteedLeaves: any[], condition: any): boolean {
    if (!condition) {
      return false;
    }
    if (condition.and) {
      return condition.and.every((c: any) => this.filterConditionImplied(guaranteedLeaves, c));
    }
    if (condition.or) {
      return condition.or.some((c: any) => this.filterConditionImplied(guaranteedLeaves, c));
    }
    if (this.filterMemberName(condition) && condition.operator) {
      return guaranteedLeaves.some(leaf => this.leafFilterImplies(leaf, condition));
    }
    return false;
  }

  /**
   * Returns true when the rows allowed by `queryFilters` are guaranteed to be a
   * subset of the rows allowed by `maskFilter` — i.e. the query is already at
   * least as restrictive as the (row-security) mask filter.
   */
  protected queryFiltersImplyFilter(queryFilters: any[], maskFilter: any): boolean {
    const guaranteedLeaves = this.collectGuaranteedFilterLeaves(queryFilters);
    if (guaranteedLeaves.length === 0) {
      return false;
    }
    return this.filterConditionImplied(guaranteedLeaves, maskFilter);
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
            } else if (!policy.memberMasking) {
              // If there's no memberLevel and no memberMasking policy, all members are visible
              return true;
            }
            if (policy.memberMasking) {
              if (policy.memberMasking.includesMembers.includes(item.name) &&
               !policy.memberMasking.excludesMembers.includes(item.name)) {
                return true;
              }
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
    options: { includeCompilerId?: boolean; includeViewGroups?: boolean; skipVisibilityPatch?: boolean; requestId?: string } = {}
  ): Promise<any> {
    const { includeCompilerId, includeViewGroups, skipVisibilityPatch, ...restOptions } = options;
    const compilers = await this.getCompilers(restOptions);
    const { cubes } = compilers.metaTransformer;

    if (skipVisibilityPatch) {
      if (includeCompilerId || includeViewGroups) {
        const result: any = { cubes, compilerId: compilers.compilerId };
        if (includeViewGroups) {
          result.viewGroups = compilers.metaTransformer.viewGroups;
        }
        return result;
      }
      return cubes;
    }

    const { visibilityMaskHash, cubes: patchedCubes } = await this.patchVisibilityByAccessPolicy(
      compilers,
      requestContext,
      cubes
    );
    if (includeCompilerId || includeViewGroups) {
      const result: any = {
        cubes: patchedCubes,
        compilerId: visibilityMaskHash ? this.mixInVisibilityMaskHash(compilers.compilerId, visibilityMaskHash) : compilers.compilerId,
      };
      if (includeViewGroups) {
        result.viewGroups = compilers.metaTransformer.viewGroups;
      }
      return result;
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
