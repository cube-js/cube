import crypto from 'crypto';
import {
  createQuery,
  compile,
  queryClass,
  PreAggregations,
  QueryFactory,
  prepareCompiler
} from '@cubejs-backend/schema-compiler';
import { v4 as uuidv4, parse as uuidParse } from 'uuid';
import { LRUCache } from 'lru-cache';
import { NativeInstance } from '@cubejs-backend/native';

export class CompilerApi {
  /**
   * Class constructor.
   * @param {SchemaFileRepository} repository
   * @param {DbTypeAsyncFn} dbType
   * @param {*} options
   */

  constructor(repository, dbType, options) {
    this.repository = repository;
    this.dbType = dbType;
    this.dialectClass = options.dialectClass;
    this.options = options || {};
    this.allowNodeRequire = options.allowNodeRequire == null ? true : options.allowNodeRequire;
    this.logger = this.options.logger;
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
    this.compiledScriptCache = new LRUCache({
      max: options.compilerCacheSize || 250,
      ttl: options.maxCompilerCacheKeepAlive,
      updateAgeOnGet: options.updateCompilerCacheKeepAlive
    });

    // proactively free up old cache values occasionally
    if (this.options.maxCompilerCacheKeepAlive) {
      this.compiledScriptCacheInterval = setInterval(
        () => this.compiledScriptCache.purgeStale(),
        this.options.maxCompilerCacheKeepAlive
      );
    }
  }

  dispose() {
    if (this.compiledScriptCacheInterval) {
      clearInterval(this.compiledScriptCacheInterval);
    }
  }

  setGraphQLSchema(schema) {
    this.graphqlSchema = schema;
  }

  getGraphQLSchema() {
    return this.graphqlSchema;
  }

  createNativeInstance() {
    return new NativeInstance();
  }

  async getCompilers({ requestId } = {}) {
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

  /**
   * Creates the compilers instances without model compilation,
   * because it could fail and no compilers will be returned.
   */
  createCompilerInstances() {
    return prepareCompiler(this.repository, {
      allowNodeRequire: this.allowNodeRequire,
      compileContext: this.compileContext,
      allowJsDuplicatePropsInSchema: this.allowJsDuplicatePropsInSchema,
      standalone: this.standalone,
      nativeInstance: this.nativeInstance,
      compiledScriptCache: this.compiledScriptCache,
    });
  }

  async compileSchema(compilerVersion, requestId) {
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
      });
      this.queryFactory = await this.createQueryFactory(compilers);

      this.logger('Compiling schema completed', {
        version: compilerVersion,
        requestId,
        duration: ((new Date()).getTime() - startCompilingTime),
      });

      return compilers;
    } catch (e) {
      this.logger('Compiling schema error', {
        version: compilerVersion,
        requestId,
        duration: ((new Date()).getTime() - startCompilingTime),
        error: (e.stack || e).toString()
      });
      throw e;
    }
  }

  async createQueryFactory(compilers) {
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

  async getDbType(dataSource = 'default') {
    return this.dbType({ dataSource, });
  }

  getDialectClass(dataSource = 'default', dbType) {
    return this.dialectClass?.({ dataSource, dbType });
  }

  async getSqlGenerator(query, dataSource) {
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

  async getSql(query, options = {}) {
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

  async getRolesFromContext(context) {
    if (!this.contextToRoles) {
      return new Set();
    }
    return new Set(await this.contextToRoles(context));
  }

  async getGroupsFromContext(context) {
    if (!this.contextToGroups) {
      return new Set();
    }
    return new Set(await this.contextToGroups(context));
  }

  userHasRole(userRoles, role) {
    return userRoles.has(role) || role === '*';
  }

  userHasGroup(userGroups, group) {
    if (Array.isArray(group)) {
      return group.some(g => userGroups.has(g) || g === '*');
    }
    return userGroups.has(group) || group === '*';
  }

  roleMeetsConditions(evaluatedConditions) {
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

  async getCubesFromQuery(query, context) {
    const sql = await this.getSql(query, { requestId: context.requestId });
    return new Set(sql.memberNames.map(memberName => memberName.split('.')[0]));
  }

  hashRequestContext(context) {
    if (!context.__hash) {
      context.__hash = crypto.createHash('md5').update(JSON.stringify(context)).digest('hex');
    }
    return context.__hash;
  }

  async getApplicablePolicies(cube, context, compilers) {
    const cache = compilers.compilerCache.getRbacCacheInstance();
    const cacheKey = `${cube.name}_${this.hashRequestContext(context)}`;
    if (!cache.has(cacheKey)) {
      const userRoles = await this.getRolesFromContext(context);
      const userGroups = await this.getGroupsFromContext(context);
      const policies = cube.accessPolicy.filter(policy => {
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
          condition => compilers.cubeEvaluator.evaluateContextFunction(cube, condition.if, context)
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

  evaluateNestedFilter(filter, cube, context, cubeEvaluator) {
    const result = {
    };
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
      result.or = filter.or.map(f => this.evaluateNestedFilter(f, cube, context, cubeEvaluator));
    }
    if (filter.and) {
      result.and = filter.and.map(f => this.evaluateNestedFilter(f, cube, context, cubeEvaluator));
    }
    return result;
  }

  /**
   * This method rewrites the query according to RBAC row level security policies.
   *
   * If RBAC is enabled, it looks at all the Cubes from the query with accessPolicy defined.
   * It extracts all policies applicable to for the current user context (contextToRoles() + conditions).
   * It then generates an rls filter by
   * - combining all filters for the same role with AND
   * - combining all filters for different roles with OR
   * - combining cube and view filters with AND
  */
  async applyRowLevelSecurity(query, evaluatedQuery, context) {
    const compilers = await this.getCompilers({ requestId: context.requestId });
    const { cubeEvaluator } = compilers;

    if (!cubeEvaluator.isRbacEnabled()) {
      return { query, denied: false };
    }

    const queryCubes = await this.getCubesFromQuery(evaluatedQuery, context);

    // We collect Cube and View filters separately because they have to be
    // applied in "two layers": first Cube filters, then View filters on top
    const cubeFiltersPerCubePerRole = {};
    const viewFiltersPerCubePerRole = {};
    const hasAllowAllForCube = {};

    for (const cubeName of queryCubes) {
      const cube = cubeEvaluator.cubeFromPath(cubeName);
      const filtersMap = cube.isView ? viewFiltersPerCubePerRole : cubeFiltersPerCubePerRole;

      if (cubeEvaluator.isRbacEnabledForCube(cube)) {
        let hasAccessPermission = false;
        const userPolicies = await this.getApplicablePolicies(cube, context, compilers);

        for (const policy of userPolicies) {
          hasAccessPermission = true;
          (policy?.rowLevel?.filters || []).forEach(filter => {
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
            // We don't have a way to add an "all alloed" filter like `WHERE 1 = 1` or something.
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
          });
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

  removeEmptyFilters(filter) {
    if (filter?.and) {
      const and = filter.and.map(f => this.removeEmptyFilters(f)).filter(f => f);
      return and.length > 1 ? { and } : and.at(0) || null;
    }
    if (filter?.or) {
      const or = filter.or.map(f => this.removeEmptyFilters(f)).filter(f => f);
      return or.length > 1 ? { or } : or.at(0) || null;
    }
    return filter;
  }

  buildFinalRlsFilter(cubeFiltersPerCubePerRole, viewFiltersPerCubePerRole, hasAllowAllForCube) {
    // - delete all filters for cubes where the user has allowAll
    // - combine the rest into per policy maps (policies can be role-based or group-based)
    // - join all filters for the same policy with AND
    // - join all filters for different policies with OR
    // - join cube and view filters with AND

    const policyReducer = (filtersMap) => (acc, cubeName) => {
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

  async compilerCacheFn(requestId, key, path) {
    const compilers = await this.getCompilers({ requestId });
    if (this.sqlCache) {
      return (subKey, cacheFn) => compilers.compilerCache.getQueryCache(key).cache(path.concat(subKey), cacheFn);
    } else {
      return (subKey, cacheFn) => cacheFn();
    }
  }

  /**
   *
   * @param {unknown} filter
   * @returns {Promise<Array<PreAggregationInfo>>}
   */
  async preAggregations(filter) {
    const { cubeEvaluator } = await this.getCompilers();
    return cubeEvaluator.preAggregations(filter);
  }

  async scheduledPreAggregations() {
    const { cubeEvaluator } = await this.getCompilers();
    return cubeEvaluator.scheduledPreAggregations();
  }

  async createQueryByDataSource(compilers, query, dataSource, dbType) {
    if (!dbType) {
      dbType = await this.getDbType(dataSource);
    }

    return this.createQuery(compilers, dbType, this.getDialectClass(dataSource, dbType), query);
  }

  createQuery(compilers, dbType, dialectClass, query) {
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
  async patchVisibilityByAccessPolicy(compilers, context, cubes) {
    const isMemberVisibleInContext = {};
    const { cubeEvaluator } = compilers;

    if (!cubeEvaluator.isRbacEnabled()) {
      return { cubes, visibilityMaskHash: null };
    }

    for (const cube of cubes) {
      const evaluatedCube = cubeEvaluator.cubeFromPath(cube.config.name);
      if (cubeEvaluator.isRbacEnabledForCube(evaluatedCube)) {
        const applicablePolicies = await this.getApplicablePolicies(evaluatedCube, context, compilers);

        const computeMemberVisibility = (item) => {
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

    const visibilityPatcherForCube = (cube) => {
      const evaluatedCube = cubeEvaluator.cubeFromPath(cube.config.name);
      if (!cubeEvaluator.isRbacEnabledForCube(evaluatedCube)) {
        return (item) => item;
      }
      return (item) => ({
        ...item,
        isVisible: item.isVisible && isMemberVisibleInContext[item.name],
        public: item.public && isMemberVisibleInContext[item.name]
      });
    };

    const visibiliyMask = JSON.stringify(isMemberVisibleInContext, Object.keys(isMemberVisibleInContext).sort());
    // This hash will be returned along the modified meta config and can be used
    // to distinguish between different "schema versions" after DAP visibility is applied
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

  mixInVisibilityMaskHash(compilerId, visibilityMaskHash) {
    const uuidBytes = uuidParse(compilerId);
    const hashBytes = Buffer.from(visibilityMaskHash, 'hex');
    return uuidv4({ random: crypto.createHash('sha256').update(uuidBytes).update(hashBytes).digest()
      .subarray(0, 16) });
  }

  async metaConfig(requestContext, options = {}) {
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
        // By default it doesn't account for member visibility changes introduced above by DAP.
        // Here we're modifying the originila compilerId in a way that it's distinct for
        // distinct schema versions while still being a valid UUID.
        compilerId: visibilityMaskHash ? this.mixInVisibilityMaskHash(compilers.compilerId, visibilityMaskHash) : compilers.compilerId,
      };
    }
    return patchedCubes;
  }

  async metaConfigExtended(requestContext, options) {
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

  async compilerId(options = {}) {
    return (await this.getCompilers(options)).compilerId;
  }

  async cubeNameToDataSource(query) {
    const { cubeEvaluator } = await this.getCompilers({ requestId: query.requestId });
    return cubeEvaluator
      .cubeNames()
      .map(
        (cube) => ({ [cube]: cubeEvaluator.cubeFromPath(cube).dataSource || 'default' })
      ).reduce((a, b) => ({ ...a, ...b }), {});
  }

  async memberToDataSource(query) {
    const { cubeEvaluator } = await this.getCompilers({ requestId: query.requestId });

    const entries = cubeEvaluator
      .cubeNames()
      .flatMap(cube => {
        const cubeDef = cubeEvaluator.cubeFromPath(cube);
        if (cubeDef.isView) {
          const viewName = cubeDef.name;
          return cubeDef.includedMembers.map(included => {
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

  async dataSources(orchestratorApi, query) {
    const cubeNameToDataSource = await this.cubeNameToDataSource(query || { requestId: `datasources-${uuidv4()}` });

    let dataSources = Object.keys(cubeNameToDataSource).map(c => cubeNameToDataSource[c]);

    dataSources = [...new Set(dataSources)];

    dataSources = await Promise.all(
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
      dataSources: dataSources.filter((source) => source),
    };
  }

  canUsePreAggregationForTransformedQuery(transformedQuery, refs) {
    return PreAggregations.canUsePreAggregationForTransformedQueryFn(transformedQuery, refs);
  }
}
