import crypto from 'crypto';
import R from 'ramda';
import { createQuery, compile, queryClass, PreAggregations, QueryFactory } from '@cubejs-backend/schema-compiler';
import { v4 as uuidv4 } from 'uuid';
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
    this.compileContext = options.compileContext;
    this.allowJsDuplicatePropsInSchema = options.allowJsDuplicatePropsInSchema;
    this.sqlCache = options.sqlCache;
    this.standalone = options.standalone;
    this.nativeInstance = this.createNativeInstance();
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

    if (this.options.devServer) {
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

    const cubeToQueryClass = R.fromPairs(
      await Promise.all(
        cubeEvaluator.cubeNames().map(async cube => {
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
    return this.dialectClass && this.dialectClass({ dataSource, dbType });
  }

  async getSqlGenerator(query, dataSource) {
    const dbType = await this.getDbType(dataSource);
    const compilers = await this.getCompilers({ requestId: query.requestId });
    let sqlGenerator = await this.createQueryByDataSource(compilers, query, dataSource, dbType);

    if (!sqlGenerator) {
      throw new Error(`Unknown dbType: ${dbType}`);
    }

    dataSource = compilers.compiler.withQuery(sqlGenerator, () => sqlGenerator.dataSource);
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
        throw new Error(`Can't find dialect for '${dataSource}' data source: ${_dbType}`);
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

  userHasRole(userRoles, role) {
    return userRoles.has(role) || role === '*';
  }

  roleMeetsConditions(evaluatedConditions) {
    if (evaluatedConditions && evaluatedConditions.length) {
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
      const policies = cube.accessPolicy.filter(policy => {
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

  evaluateNestedFilter(filter, cube, context, cubeEvaluator) {
    const result = {
    };
    if (filter.memberReference) {
      const evaluatedValues = cubeEvaluator.evaluateContextFunction(
        cube,
        filter.values,
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
        let hasRoleWithAccess = false;
        const userPolicies = await this.getApplicablePolicies(cube, context, compilers);

        for (const policy of userPolicies) {
          hasRoleWithAccess = true;
          (policy?.rowLevel?.filters || []).forEach(filter => {
            filtersMap[cubeName] = filtersMap[cubeName] || {};
            filtersMap[cubeName][policy.role] = filtersMap[cubeName][policy.role] || [];
            filtersMap[cubeName][policy.role].push(
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

        if (!hasRoleWithAccess) {
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
    query.filters = query.filters || [];
    query.filters.push(rlsFilter);
    return { query, denied: false };
  }

  buildFinalRlsFilter(cubeFiltersPerCubePerRole, viewFiltersPerCubePerRole, hasAllowAllForCube) {
    // - delete all filters for cubes where the user has allowAll
    // - combine the rest into per role maps
    // - join all filters for the same role with AND
    // - join all filters for different roles with OR
    // - join cube and view filters with AND

    const roleReducer = (filtersMap) => (acc, cubeName) => {
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

    return {
      and: [{
        or: Object.keys(cubeFiltersPerRole).map(role => ({
          and: cubeFiltersPerRole[role]
        }))
      }, {
        or: Object.keys(viewFiltersPerRole).map(role => ({
          and: viewFiltersPerRole[role]
        }))
      }]
    };
  }

  async compilerCacheFn(requestId, key, path) {
    const compilers = await this.getCompilers({ requestId });
    if (this.sqlCache) {
      return (subKey, cacheFn) => compilers.compilerCache.getQueryCache(key).cache(path.concat(subKey), cacheFn);
    } else {
      return (subKey, cacheFn) => cacheFn();
    }
  }

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
      return cubes;
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

    return cubes
      .map((cube) => ({
        config: {
          ...cube.config,
          measures: cube.config.measures?.map(visibilityPatcherForCube(cube)),
          dimensions: cube.config.dimensions?.map(visibilityPatcherForCube(cube)),
          segments: cube.config.segments?.map(visibilityPatcherForCube(cube)),
          hierarchies: cube.config.hierarchies?.map(visibilityPatcherForCube(cube)),
        },
      }));
  }

  async metaConfig(requestContext, options = {}) {
    const { includeCompilerId, ...restOptions } = options;
    const compilers = await this.getCompilers(restOptions);
    const { cubes } = compilers.metaTransformer;
    const patchedCubes = await this.patchVisibilityByAccessPolicy(
      compilers,
      requestContext,
      cubes
    );
    if (includeCompilerId) {
      return {
        cubes: patchedCubes,
        compilerId: compilers.compilerId,
      };
    }
    return patchedCubes;
  }

  async metaConfigExtended(requestContext, options) {
    const compilers = await this.getCompilers(options);
    const patchedCubes = await this.patchVisibilityByAccessPolicy(
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
