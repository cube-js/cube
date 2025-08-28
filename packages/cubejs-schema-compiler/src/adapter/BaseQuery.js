/* eslint-disable no-unused-vars,prefer-template */

/**
 * @fileoverview BaseQuery class definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

import cronParser from 'cron-parser';
import inflection from 'inflection';
import moment from 'moment-timezone';
import R from 'ramda';

import {
  buildSqlAndParams as nativeBuildSqlAndParams,
} from '@cubejs-backend/native';
import {
  FROM_PARTITION_RANGE,
  MAX_SOURCE_ROW_LIMIT,
  QueryAlias,
  getEnv,
  localTimestampToUtc,
  timeSeries as timeSeriesBase,
  timeSeriesFromCustomInterval,
  parseSqlInterval,
  findMinGranularityDimension
} from '@cubejs-backend/shared';

import { CubeSymbols } from '../compiler/CubeSymbols';
import { UserError } from '../compiler/UserError';
import { SqlParser } from '../parser/SqlParser';
import { BaseDimension } from './BaseDimension';
import { BaseFilter } from './BaseFilter';
import { BaseGroupFilter } from './BaseGroupFilter';
import { BaseMeasure } from './BaseMeasure';
import { BaseSegment } from './BaseSegment';
import { BaseTimeDimension } from './BaseTimeDimension';
import { Granularity } from './Granularity';
import { ParamAllocator } from './ParamAllocator';
import { PreAggregations } from './PreAggregations';

const DEFAULT_PREAGGREGATIONS_SCHEMA = 'stb_pre_aggregations';

const standardGranularitiesParents = {
  year: ['year', 'quarter', 'month', 'day', 'hour', 'minute', 'second'],
  quarter: ['quarter', 'month', 'day', 'hour', 'minute', 'second'],
  month: ['month', 'day', 'hour', 'minute', 'second'],
  week: ['week', 'day', 'hour', 'minute', 'second'],
  day: ['day', 'hour', 'minute', 'second'],
  hour: ['hour', 'minute', 'second'],
  minute: ['minute', 'second'],
  second: ['second']
};

const SecondsDurations = {
  week: 60 * 60 * 24 * 7,
  day: 60 * 60 * 24,
  hour: 60 * 60,
  minute: 60,
  second: 1
};

/**
 * Set of the schema compilers.
 * @typedef {Object} Compilers
 * @property {import('../compiler/DataSchemaCompiler').DataSchemaCompiler} compiler
 * @property {import('../compiler/CubeToMetaTransformer').CubeToMetaTransformer} metaTransformer
 * @property {import('../compiler/CubeEvaluator').CubeEvaluator} cubeEvaluator
 * @property {import('../compiler/ContextEvaluator').ContextEvaluator} contextEvaluator
 * @property {import('../compiler/JoinGraph').JoinGraph} joinGraph
 * @property {import('../compiler/CompilerCache').CompilerCache} compilerCache
 * @property {*} headCommitId
 */

/**
 * @typedef {Object} JoinRoot
 * @property {string} sql
 * @property {string} alias
 */

/**
 * @typedef {Object} JoinItem
 * @property {string} sql
 * @property {string} alias
 * @property {string} on
 */

/**
 * @typedef {[JoinRoot, ...JoinItem]} JoinChain
 */

/**
 * BaseQuery class. BaseQuery object encapsulates the logic of
 * transforming an incoming to a specific cube request to the
 * SQL-query string.
 *
 * This class is a parent class for the set of dialect specific
 * query adapters (for ex. MysqlQuery, OracleQuery, etc.).
 *
 * You should never instantiate this class manually. Instead, you
 * should use {@code CompilerApi#getDialectClass} method, which
 * should return query object based on the datasource, database type
 * and {@code CompilerApi} configuration.
 */
export class BaseQuery {
  /** @type {import('./PreAggregations').PreAggregations} */
  preAggregations;

  /** @type {import('./BaseMeasure').BaseMeasure[]} */
  measures;

  /** @type {import('./BaseDimension').BaseDimension[]} */
  dimensions;

  /** @type {import('./BaseDimension').BaseDimension[]} */
  multiStageDimensions;

  /** @type {import('./BaseTimeDimension').BaseTimeDimension[]} */
  multiStageTimeDimensions;

  /** @type {import('./BaseSegment').BaseSegment[]} */
  segments;

  /** @type {(BaseFilter|BaseGroupFilter)[]} */
  filters;

  /** @type {(BaseFilter|BaseGroupFilter)[]} */
  measureFilters;

  /** @type {import('./BaseTimeDimension').BaseTimeDimension[]} */
  timeDimensions;

  /** @type {import('../compiler/JoinGraph').FinishedJoinTree} */
  join;

  /**
   * BaseQuery class constructor.
   * @param {Compilers|*} compilers
   * @param {*} options
   */
  constructor(compilers, options) {
    this.compilers = compilers;
    /** @type {import('../compiler/CubeEvaluator').CubeEvaluator} */
    this.cubeEvaluator = compilers.cubeEvaluator;
    /** @type {import('../compiler/JoinGraph').JoinGraph} */
    this.joinGraph = compilers.joinGraph;
    this.options = options || {};

    this.orderHashToString = this.orderHashToString.bind(this);
    this.defaultOrder = this.defaultOrder.bind(this);
    /** @type {ParamAllocator} */
    this.paramAllocator = this.options.paramAllocator || this.newParamAllocator(this.options.expressionParams);
    this.initFromOptions();
  }

  extractDimensionsAndMeasures(filters = []) {
    if (!filters) {
      return [];
    }
    let allFilters = [];
    filters.forEach(f => {
      if (f.and) {
        allFilters = allFilters.concat(this.extractDimensionsAndMeasures(f.and));
      } else if (f.or) {
        allFilters = allFilters.concat(this.extractDimensionsAndMeasures(f.or));
      } else if (!f.member && !f.dimension) {
        throw new UserError(`member attribute is required for filter ${JSON.stringify(f)}`);
      } else if (this.cubeEvaluator.isMeasure(f.member || f.dimension)) {
        allFilters.push({ measure: f.member || f.dimension });
      } else {
        allFilters.push({ dimension: f.member || f.dimension });
      }
    });

    return allFilters;
  }

  keepFilters(filters = [], fn) {
    return filters.map(f => {
      if (f.and) {
        return { and: this.keepFilters(f.and, fn) };
      } else if (f.or) {
        return { or: this.keepFilters(f.or, fn) };
      } else if (!f.member && !f.dimension) {
        throw new UserError(`member attribute is required for filter ${JSON.stringify(f)}`);
      } else {
        return fn(f.member || f.dimension || f.measure) ? f : null;
      }
    }).filter(f => !!f);
  }

  extractFiltersAsTree(filters = []) {
    if (!filters) {
      return [];
    }

    return filters.map(f => {
      if (f.and || f.or) {
        let operator = 'or';
        if (f.and) {
          operator = 'and';
        }
        const data = this.extractDimensionsAndMeasures(f[operator]);
        const dimension = data.filter(e => !!e.dimension).map(e => e.dimension);
        const measure = data.filter(e => !!e.measure).map(e => e.measure);
        if (dimension.length && !measure.length) {
          return {
            values: this.extractFiltersAsTree(f[operator]),
            operator,
            dimensionGroup: true,
            measure: null,
          };
        }
        if (!dimension.length && measure.length) {
          return {
            values: this.extractFiltersAsTree(f[operator]),
            operator,
            dimension: null,
            measureGroup: true,
          };
        }
        if (!dimension.length && !measure.length) {
          return {
            values: [],
            operator,
          };
        }
        throw new UserError(`You cannot use dimension and measure in same condition: ${JSON.stringify(f)}`);
      }

      if (!f.member && !f.dimension) {
        throw new UserError(`member attribute is required for filter ${JSON.stringify(f)}`);
      }

      if (this.cubeEvaluator.isMeasure(f.member || f.dimension)) {
        return Object.assign({}, f, {
          dimension: null,
          measure: f.member || f.dimension
        });
      }

      return Object.assign({}, f, {
        measure: null,
        dimension: f.member || f.dimension
      });
    });
  }

  /**
   * @protected
   */
  initFromOptions() {
    this.contextSymbols = {
      securityContext: {},
      ...this.options.contextSymbols,
    };
    this.compilerCache = this.compilers.compiler.compilerCache;
    this.queryCache = this.compilerCache.getQueryCache({
      measures: this.options.measures,
      dimensions: this.options.dimensions,
      timeDimensions: this.options.timeDimensions,
      filters: this.options.filters,
      segments: this.options.segments,
      order: this.options.order,
      contextSymbols: this.options.contextSymbols,
      timezone: this.options.timezone,
      limit: this.options.limit,
      offset: this.options.offset,
      rowLimit: this.options.rowLimit,
      preAggregationsSchema: this.options.preAggregationsSchema,
      className: this.constructor.name,
      externalClassName: this.options.externalQueryClass?.name,
      preAggregationQuery: this.options.preAggregationQuery,
      disableExternalPreAggregations: this.options.disableExternalPreAggregations,
      useOriginalSqlPreAggregationsInPreAggregation: this.options.useOriginalSqlPreAggregationsInPreAggregation,
      cubeLatticeCache: this.options.cubeLatticeCache, // TODO too heavy for key
      historyQueries: this.options.historyQueries, // TODO too heavy for key
      ungrouped: this.options.ungrouped,
      memberToAlias: this.options.memberToAlias,
      expressionParams: this.options.expressionParams,
      convertTzForRawTimeDimension: this.options.convertTzForRawTimeDimension,
      from: this.options.from,
      multiStageQuery: this.options.multiStageQuery,
      multiStageDimensions: this.options.multiStageDimensions,
      multiStageTimeDimensions: this.options.multiStageTimeDimensions,
      subqueryJoins: this.options.subqueryJoins,
      joinHints: this.options.joinHints,
    });
    this.from = this.options.from;
    this.multiStageQuery = this.options.multiStageQuery;
    this.timezone = this.options.timezone;
    this.rowLimit = this.options.rowLimit;
    this.offset = this.options.offset;
    /** @type {import('./PreAggregations').PreAggregations} */
    this.preAggregations = this.newPreAggregations();
    /** @type {import('./BaseMeasure').BaseMeasure[]} */
    this.measures = (this.options.measures || []).map(this.newMeasure.bind(this));
    /** @type {import('./BaseDimension').BaseDimension[]} */
    this.dimensions = (this.options.dimensions || []).map(this.newDimension.bind(this));
    /** @type {import('./BaseDimension').BaseDimension[]} */
    this.multiStageDimensions = (this.options.multiStageDimensions || []).map(this.newDimension.bind(this));
    /** @type {import('./BaseTimeDimension').BaseTimeDimension[]} */
    this.multiStageTimeDimensions = (this.options.multiStageTimeDimensions || []).map(this.newTimeDimension.bind(this));
    /** @type {import('./BaseSegment').BaseSegment[]} */
    this.segments = (this.options.segments || []).map(this.newSegment.bind(this));

    const filters = this.extractFiltersAsTree(this.options.filters || []);

    // measure_filter (the one extracted from filters parameter on measure and
    // used in drill-downs) should go to WHERE instead of HAVING
    /** @type {(BaseFilter|BaseGroupFilter)[]} */
    this.filters = filters.filter(f => f.dimensionGroup || f.dimension || f.operator === 'measure_filter' || f.operator === 'measureFilter').map(this.initFilter.bind(this));
    /** @type {(BaseFilter|BaseGroupFilter)[]} */
    this.measureFilters = filters.filter(f => (f.measureGroup || f.measure) && f.operator !== 'measure_filter' && f.operator !== 'measureFilter').map(this.initFilter.bind(this));
    /** @type {import('./BaseTimeDimension').BaseTimeDimension[]} */
    this.timeDimensions = (this.options.timeDimensions || []).map(dimension => {
      if (!dimension.dimension) {
        const join = this.joinGraph.buildJoin(this.collectJoinHints(true));
        if (!join) {
          return undefined;
        }

        // eslint-disable-next-line prefer-destructuring
        dimension.dimension = this.cubeEvaluator.timeDimensionPathsForCube(join.root)[0];
        if (!dimension.dimension) {
          return undefined;
        }
      }
      return dimension;
    }).filter(R.identity).map(this.newTimeDimension.bind(this));
    this.allFilters = this.timeDimensions.concat(this.segments).concat(this.filters);
    /**
     * For now this might come only from SQL API, it might be some queries that uses measures and filters to
     * get the dimensions that are then used as join conditions to get the final results.
     * As consequence - if there are such sub query joins - pre-aggregations can't be used.
     * @type {Array<{sql: string, on: {expression: Function}, joinType: 'LEFT' | 'INNER', alias: string}>}
     */
    this.customSubQueryJoins = this.options.subqueryJoins ?? [];
    this.useNativeSqlPlanner = this.options.useNativeSqlPlanner ?? getEnv('nativeSqlPlanner');
    this.canUseNativeSqlPlannerPreAggregation = getEnv('nativeSqlPlannerPreAggregations');
    if (this.useNativeSqlPlanner && !this.canUseNativeSqlPlannerPreAggregation && !this.neverUseSqlPlannerPreaggregation()) {
      const fullAggregateMeasures = this.fullKeyQueryAggregateMeasures({ hasMultipliedForPreAggregation: true });

      this.canUseNativeSqlPlannerPreAggregation = fullAggregateMeasures.multiStageMembers.length > 0;
    }
    this.queryLevelJoinHints = this.options.joinHints ?? [];
    this.prebuildJoin();

    this.cubeAliasPrefix = this.options.cubeAliasPrefix;
    this.preAggregationsSchemaOption = this.options.preAggregationsSchema ?? DEFAULT_PREAGGREGATIONS_SCHEMA;
    this.externalQueryClass = this.options.externalQueryClass;

    // Set the default order only when options.order is not provided at all
    // if options.order is set (empty array [] or with data) - use it as is
    this.order = this.options.order ?? this.defaultOrder();

    this.initUngrouped();
  }

  // Temporary workaround to avoid checking for multistage in CubeStoreQuery, since that could lead to errors when HLL functions are present in the query.
  neverUseSqlPlannerPreaggregation() {
    return false;
  }

  prebuildJoin() {
    try {
      // TODO allJoinHints should contain join hints form pre-agg
      this.join = this.joinGraph.buildJoin(this.allJoinHints);
      /**
       * @type {Record<string, string[]>}
       */
      const queryJoinGraph = {};
      for (const { originalFrom, originalTo } of (this.join?.joins || [])) {
        if (!queryJoinGraph[originalFrom]) {
          queryJoinGraph[originalFrom] = [];
        }
        queryJoinGraph[originalFrom].push(originalTo);
      }
      this.joinGraphPaths = queryJoinGraph || {};
    } catch (e) {
      if (this.useNativeSqlPlanner) {
        // Tesseract doesn't require join to be prebuilt and there's a case where single join can't be built for multi-fact query
        // But we need this join for a fallback when using pre-aggregations. So we’ll try to obtain the join but ignore any errors (which may occur if the query is a multi-fact one).
      } else {
        throw e;
      }
    }
  }

  cacheValue(key, fn, { contextPropNames, inputProps, cache } = {}) {
    const currentContext = this.safeEvaluateSymbolContext();
    if (contextPropNames) {
      const contextKey = {};
      for (let i = 0; i < contextPropNames.length; i++) {
        contextKey[contextPropNames[i]] = currentContext[contextPropNames[i]];
      }
      key = key.concat([JSON.stringify(contextKey)]);
    }
    const { value, resultProps } = (cache || this.compilerCache).cache(
      key,
      () => {
        if (inputProps) {
          return {
            value: this.evaluateSymbolSqlWithContext(fn, inputProps),
            resultProps: inputProps
          };
        }
        return { value: fn() };
      }
    );
    if (resultProps) {
      Object.keys(resultProps).forEach(k => {
        if (Array.isArray(currentContext[k])) {
          // eslint-disable-next-line prefer-spread
          currentContext[k].push.apply(currentContext[k], resultProps[k]);
        } else if (currentContext[k]) {
          Object.keys(currentContext[k]).forEach(innerKey => {
            currentContext[k][innerKey] = resultProps[k][innerKey];
          });
        }
      });
    }
    return value;
  }

  get allCubeNames() {
    if (!this.collectedCubeNames) {
      this.collectedCubeNames = this.collectCubeNames();
    }
    return this.collectedCubeNames;
  }

  /**
   *
   * @returns {Array<string | Array<string>>}
   */
  get allJoinHints() {
    if (!this.collectedJoinHints) {
      const [rootOfJoin, ...allMembersJoinHints] = this.collectJoinHintsFromMembers(this.allMembersConcat(false));
      const customSubQueryJoinHints = this.collectJoinHintsFromMembers(this.joinMembersFromCustomSubQuery());
      let joinMembersJoinHints = this.collectJoinHintsFromMembers(this.joinMembersFromJoin(this.join));

      // One cube may join the other cube via transitive joined cubes,
      // members from which are referenced in the join `on` clauses.
      // We need to collect such join hints and push them upfront of the joining one
      // but only if they don't exist yet. Cause in other case we might affect what
      // join path will be constructed in join graph.
      // It is important to use queryLevelJoinHints during the calculation if it is set.

      const constructJH = () => {
        const filteredJoinMembersJoinHints = joinMembersJoinHints.filter(m => !allMembersJoinHints.includes(m));
        return [
          ...this.queryLevelJoinHints,
          ...(rootOfJoin ? [rootOfJoin] : []),
          ...filteredJoinMembersJoinHints,
          ...allMembersJoinHints,
          ...customSubQueryJoinHints,
        ];
      };

      let prevJoins = this.join;
      let prevJoinMembersJoinHints = joinMembersJoinHints;
      let newJoin = this.joinGraph.buildJoin(constructJH());

      const isOrderPreserved = (base, updated) => {
        const common = base.filter(value => updated.includes(value));
        const bFiltered = updated.filter(value => common.includes(value));

        return common.every((x, i) => x === bFiltered[i]);
      };

      const isJoinTreesEqual = (a, b) => {
        if (!a || !b || a.root !== b.root || a.joins.length !== b.joins.length) {
          return false;
        }

        // We don't care about the order of joins on the same level, so
        // we can compare them as sets.
        const aJoinsSet = new Set(a.joins.map(j => `${j.originalFrom}->${j.originalTo}`));
        const bJoinsSet = new Set(b.joins.map(j => `${j.originalFrom}->${j.originalTo}`));

        if (aJoinsSet.size !== bJoinsSet.size) {
          return false;
        }

        for (const val of aJoinsSet) {
          if (!bJoinsSet.has(val)) {
            return false;
          }
        }

        return true;
      };

      // Safeguard against infinite loop in case of cyclic joins somehow managed to slip through
      let cnt = 0;

      while (newJoin?.joins.length > 0 && !isJoinTreesEqual(prevJoins, newJoin) && cnt < 10000) {
        prevJoins = newJoin;
        joinMembersJoinHints = this.collectJoinHintsFromMembers(this.joinMembersFromJoin(newJoin));
        if (!isOrderPreserved(prevJoinMembersJoinHints, joinMembersJoinHints)) {
          throw new UserError(`Can not construct joins for the query, potential loop detected: ${prevJoinMembersJoinHints.join('->')} vs ${joinMembersJoinHints.join('->')}`);
        }
        newJoin = this.joinGraph.buildJoin(constructJH());
        prevJoinMembersJoinHints = joinMembersJoinHints;
        cnt++;
      }

      if (cnt >= 10000) {
        throw new UserError('Can not construct joins for the query, potential loop detected');
      }

      this.collectedJoinHints = R.uniq(constructJH());
    }
    return this.collectedJoinHints;
  }

  get dataSource() {
    const dataSources = R.uniq(this.allCubeNames.map(c => this.cubeDataSource(c)));
    if (dataSources.length > 1 && !this.externalPreAggregationQuery()) {
      throw new UserError(`To join across data sources use rollupJoin with Cube Store. If rollupJoin is defined, this error indicates it doesn't match the query. Please use Rollup Designer to verify it's definition. Found data sources: ${dataSources.join(', ')}`);
    }
    return dataSources[0];
  }

  cubeDataSource(cube) {
    return this.cubeEvaluator.cubeFromPath(cube).dataSource || 'default';
  }

  get aliasNameToMember() {
    return R.fromPairs(
      this.measures.map(m => [m.unescapedAliasName(), m.measure]).concat(
        this.dimensions.map(m => [m.unescapedAliasName(), m.dimension])
      ).concat(
        this.timeDimensions.filter(m => !!m.granularity)
          .map(m => [m.unescapedAliasName(), `${m.dimension}.${m.granularity}`])
      )
    );
  }

  initUngrouped() {
    this.ungrouped = this.options.ungrouped;
    if (this.ungrouped) {
      if (!this.options.allowUngroupedWithoutPrimaryKey && this.join) {
        const cubes = R.uniq([this.join.root].concat(this.join.joins.map(j => j.originalTo)));
        const primaryKeyNames = cubes.flatMap(c => this.primaryKeyNames(c));
        const missingPrimaryKeys = primaryKeyNames.filter(key => !this.dimensions.find(d => d.dimension === key));
        if (missingPrimaryKeys.length) {
          throw new UserError(`Ungrouped query requires primary keys to be present in dimensions: ${missingPrimaryKeys.map(k => `'${k}'`).join(', ')}. Pass allowUngroupedWithoutPrimaryKey option to disable this check.`);
        }
      }
      if (this.measureFilters.length) {
        throw new UserError('Measure filters aren\'t allowed in ungrouped query');
      }
    }
  }

  get subQueryDimensions() {
    // eslint-disable-next-line no-underscore-dangle
    if (!this._subQueryDimensions) {
      // eslint-disable-next-line no-underscore-dangle
      this._subQueryDimensions = this.collectFromMembers(
        false,
        this.collectSubQueryDimensionsFor.bind(this),
        'collectSubQueryDimensionsFor'
      );
    }
    // eslint-disable-next-line no-underscore-dangle
    return this._subQueryDimensions;
  }

  get asSyntaxTable() {
    return 'AS';
  }

  get asSyntaxJoin() {
    return 'AS';
  }

  defaultOrder() {
    if (this.options.preAggregationQuery || this.options.totalQuery) {
      return [];
    }

    const res = [];

    const granularity = this.timeDimensions.find(d => d.granularity);

    if (granularity) {
      res.push({
        id: granularity.dimension,
        desc: false,
      });
    } else if (this.measures.length > 0 && this.dimensions.length > 0) {
      const firstMeasure = this.measures[0];
      const id = firstMeasure.expressionName ?? firstMeasure.measure;

      res.push({ id, desc: true });
    } else if (this.dimensions.length > 0) {
      const dim = this.dimensions[0];
      res.push({
        id: dim.expressionName ?? dim.dimension,
        desc: false,
      });
    }

    return res;
  }

  /**
   *
   * @param measurePath
   * @returns {BaseMeasure}
   */
  newMeasure(measurePath) {
    return new BaseMeasure(this, measurePath);
  }

  /**
   *
   * @param dimensionPath
   * @returns {BaseDimension}
   */
  newDimension(dimensionPath) {
    if (typeof dimensionPath === 'string') {
      const memberArr = dimensionPath.split('.');
      if (memberArr.length > 3 &&
            memberArr[memberArr.length - 2] === 'granularities' &&
            this.cubeEvaluator.isDimension(memberArr.slice(0, -2))) {
        return this.newTimeDimension(
          {
            dimension: this.cubeEvaluator.pathFromArray(memberArr.slice(0, -2)),
            granularity: memberArr[memberArr.length - 1]
          }
        );
      }
    }
    return new BaseDimension(this, dimensionPath);
  }

  /**
   *
   * @param segmentPath
   * @returns {BaseSegment}
   */
  newSegment(segmentPath) {
    return new BaseSegment(this, segmentPath);
  }

  /**
   * @returns {BaseGroupFilter|BaseFilter}
   */
  initFilter(filter) {
    if (filter.operator === 'and' || filter.operator === 'or') {
      filter.values = filter.values.map(this.initFilter.bind(this));
      return this.newGroupFilter(filter);
    }

    return this.newFilter(filter);
  }

  /**
   * @returns {BaseFilter}
   */
  newFilter(filter) {
    return new BaseFilter(this, filter);
  }

  /**
   *
   * @param filter
   * @returns {BaseGroupFilter}
   */
  newGroupFilter(filter) {
    return new BaseGroupFilter(filter);
  }

  /**
   * @param timeDimension
   * @return {BaseTimeDimension}
   */
  newTimeDimension(timeDimension) {
    return new BaseTimeDimension(this, timeDimension);
  }

  /**
   *
   * @param expressionParams
   * @returns {ParamAllocator}
   */
  newParamAllocator(expressionParams) {
    return new ParamAllocator(expressionParams);
  }

  /**
   *
   * @returns {PreAggregations}
   */
  newPreAggregations() {
    return new PreAggregations(this, this.options.historyQueries || [], this.options.cubeLatticeCache);
  }

  /**
   * Wrap specified column/table name with the double quote.
   * @param {string} name
   * @returns {string}
   */
  escapeColumnName(name) {
    return `"${name}"`;
  }

  /**
   * Returns SQL query string.
   * @returns {string}
   */
  buildParamAnnotatedSql() {
    let sql;
    let preAggForQuery;
    // TODO Most probably should be called later than here but avoids errors during pre-aggregation match for now
    if (this.from) {
      return this.simpleQuery();
    }
    const hasMemberExpressions = this.allMembersConcat(false).some(m => m.isMemberExpression);

    if (!this.options.preAggregationQuery && !this.customSubQueryJoins.length && !hasMemberExpressions) {
      preAggForQuery =
        this.preAggregations.findPreAggregationForQuery();
      if (this.options.disableExternalPreAggregations && preAggForQuery?.preAggregation.external) {
        preAggForQuery = undefined;
      }
    }
    if (preAggForQuery) {
      const {
        multipliedMeasures,
        regularMeasures,
        cumulativeMeasures,
      } = this.fullKeyQueryAggregateMeasures();

      if (cumulativeMeasures.length === 0) {
        sql = this.preAggregations.rollupPreAggregation(
          preAggForQuery,
          this.measures,
          true,
        );
      } else {
        sql = this.regularAndTimeSeriesRollupQuery(
          regularMeasures,
          multipliedMeasures,
          cumulativeMeasures,
          preAggForQuery,
        );
      }
    } else {
      sql = this.fullKeyQueryAggregate();
    }
    return this.options.totalQuery
      ? this.countAllQuery(sql)
      : sql;
  }

  /**
   * Generate SQL query to calculate total number of rows of the
   * specified SQL query.
   * @param {string} sql
   * @returns {string}
   */
  countAllQuery(sql) {
    return `select count(*) ${this.escapeColumnName(QueryAlias.TOTAL_COUNT)
    } from (\n${sql
    }\n) ${this.escapeColumnName(QueryAlias.ORIGINAL_QUERY)
    }`;
  }

  regularAndTimeSeriesRollupQuery(regularMeasures, multipliedMeasures, cumulativeMeasures, preAggregationForQuery) {
    const regularAndMultiplied = regularMeasures.concat(multipliedMeasures);
    const toJoin =
      (regularAndMultiplied.length ? [
        this.withCubeAliasPrefix('main', () => this.preAggregations.rollupPreAggregation(preAggregationForQuery, regularAndMultiplied, false)),
      ] : []).concat(
        R.map(
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          ([multiplied, measure]) => this.withCubeAliasPrefix(
            `${this.aliasName(measure.measure.replace('.', '_'))}_cumulative`,
            () => this.overTimeSeriesQuery(
              (measures, filters) => this.preAggregations.rollupPreAggregation(
                preAggregationForQuery, measures, false, filters,
              ),
              measure,
              true,
            ),
          ),
        )(cumulativeMeasures),
      );
    return this.joinFullKeyQueryAggregate(multipliedMeasures, regularMeasures, cumulativeMeasures, toJoin);
  }

  externalPreAggregationQuery() {
    if (!this.options.preAggregationQuery && !this.options.disableExternalPreAggregations && this.externalQueryClass) {
      const preAggregationForQuery = this.preAggregations.findPreAggregationForQuery();
      if (preAggregationForQuery?.preAggregation.external) {
        return true;
      }
      const preAggregationsDescription = this.preAggregations.preAggregationsDescription();

      return preAggregationsDescription.length > 0 && R.all((p) => p.external, preAggregationsDescription);
    }

    return false;
  }

  newQueryWithoutNative() {
    const QueryClass = this.constructor;
    return new QueryClass(this.compilers, { ...this.options, useNativeSqlPlanner: false });
  }

  /**
   * Returns a pair of SQL query string and parameter values for the query.
   * @param {boolean} [exportAnnotatedSql] - returns annotated sql with not rendered params if true
   * @returns {[string, Array<unknown>]}
   */
  buildSqlAndParams(exportAnnotatedSql) {
    if (this.useNativeSqlPlanner) {
      let isRelatedToPreAggregation = false;

      if (!this.canUseNativeSqlPlannerPreAggregation) {
        if (this.options.preAggregationQuery) {
          isRelatedToPreAggregation = true;
        } else if (!this.options.disableExternalPreAggregations && this.externalQueryClass && this.externalPreAggregationQuery()) {
          isRelatedToPreAggregation = true;
        } else {
          let preAggForQuery =
            this.preAggregations.findPreAggregationForQuery();
          if (this.options.disableExternalPreAggregations && preAggForQuery && preAggForQuery.preAggregation.external) {
            preAggForQuery = undefined;
          }
          if (preAggForQuery) {
            isRelatedToPreAggregation = true;
          }
        }

        if (isRelatedToPreAggregation) {
          return this.newQueryWithoutNative().buildSqlAndParams(exportAnnotatedSql);
        }
      }

      return this.buildSqlAndParamsRust(exportAnnotatedSql);
    }

    if (!this.options.preAggregationQuery && !this.options.disableExternalPreAggregations && this.externalQueryClass) {
      if (this.externalPreAggregationQuery()) { // TODO performance
        return this.externalQuery().buildSqlAndParams(exportAnnotatedSql);
      }
    }

    return this.compilers.compiler.withQuery(
      this,
      () => this.cacheValue(
        ['buildSqlAndParams', exportAnnotatedSql],
        () => this.paramAllocator.buildSqlAndParams(
          this.buildParamAnnotatedSql(),
          exportAnnotatedSql,
          this.shouldReuseParams
        ),
        { cache: this.queryCache }
      )
    );
  }

  driverTools(external) {
    if (external && !this.options.disableExternalPreAggregations && this.externalQueryClass) {
      return this.externalQuery();
    }
    return this;
  }

  buildSqlAndParamsRust(exportAnnotatedSql) {
    const order = this.options.order && R.pipe(
      R.map((hash) => ((!hash || !hash.id) ? null : hash)),
      R.reject(R.isNil),
    )(this.options.order);
    const queryParams = {
      measures: this.options.measures,
      dimensions: this.options.dimensions,
      segments: this.options.segments,
      timeDimensions: this.options.timeDimensions,
      timezone: this.options.timezone,
      joinGraph: this.joinGraph,
      cubeEvaluator: this.cubeEvaluator,
      order,
      filters: this.options.filters,
      limit: this.options.limit ? this.options.limit.toString() : null,
      rowLimit: this.options.rowLimit ? this.options.rowLimit.toString() : null,
      offset: this.options.offset ? this.options.offset.toString() : null,
      baseTools: this,
      ungrouped: this.options.ungrouped,
      exportAnnotatedSql: exportAnnotatedSql === true,
      preAggregationQuery: this.options.preAggregationQuery,
      totalQuery: this.options.totalQuery,
      joinHints: this.options.joinHints,
      cubestoreSupportMultistage: this.options.cubestoreSupportMultistage ?? getEnv('cubeStoreRollingWindowJoin')
    };

    const buildResult = nativeBuildSqlAndParams(queryParams);

    if (buildResult.error) {
      if (buildResult.error.cause && buildResult.error.cause === 'User') {
        throw new UserError(buildResult.error.message);
      } else {
        throw new Error(buildResult.error.message);
      }
    }

    const res = buildResult.result;
    const [query, params, preAggregation] = res;
    // FIXME
    const paramsArray = [...params];
    if (preAggregation) {
      this.preAggregations.preAggregationForQuery = preAggregation;
    }
    return [query, paramsArray];
  }

  // FIXME Temporary solution
  findPreAggregationForQueryRust() {
    let optionsOrder = this.options.order;
    if (optionsOrder && !Array.isArray(optionsOrder)) {
      optionsOrder = [optionsOrder];
    }
    const order = optionsOrder ? R.pipe(
      R.map((hash) => ((!hash || !hash.id) ? null : hash)),
      R.reject(R.isNil),
    )(optionsOrder) : undefined;

    const queryParams = {
      measures: this.options.measures,
      dimensions: this.options.dimensions,
      segments: this.options.segments,
      timeDimensions: this.options.timeDimensions,
      timezone: this.options.timezone,
      joinGraph: this.joinGraph,
      cubeEvaluator: this.cubeEvaluator,
      order,
      filters: this.options.filters,
      limit: this.options.limit ? this.options.limit.toString() : null,
      rowLimit: this.options.rowLimit ? this.options.rowLimit.toString() : null,
      offset: this.options.offset ? this.options.offset.toString() : null,
      baseTools: this,
      ungrouped: this.options.ungrouped,
      exportAnnotatedSql: false,
      preAggregationQuery: this.options.preAggregationQuery,
      cubestoreSupportMultistage: this.options.cubestoreSupportMultistage ?? getEnv('cubeStoreRollingWindowJoin')
    };

    const buildResult = nativeBuildSqlAndParams(queryParams);

    if (buildResult.error) {
      if (buildResult.error.cause === 'User') {
        throw new UserError(buildResult.error.message);
      } else {
        throw new Error(buildResult.error.message);
      }
    }

    const [, , preAggregation] = buildResult.result;
    return preAggregation;
  }

  allCubeMembers(path) {
    const fromPath = this.cubeEvaluator.cubeFromPath(path);

    return Object.keys(fromPath.measures).concat(Object.keys(fromPath.dimensions));
  }

  getAllocatedParams() {
    return this.paramAllocator.getParams();
  }

  // FIXME helper for native generator, maybe should be moved entirely to rust
  generateTimeSeries(granularity, dateRange) {
    return timeSeriesBase(granularity, dateRange, { timestampPrecision: this.timestampPrecision() });
  }

  // FIXME helper for native generator, maybe should be moved entirely to rust
  generateCustomTimeSeries(granularityInterval, dateRange, origin) {
    return timeSeriesFromCustomInterval(granularityInterval, dateRange, moment(origin), { timestampPrecision: this.timestampPrecision() });
  }

  getPreAggregationByName(cube, preAggregationName) {
    return this.preAggregations.getRollupPreAggregationByName(cube, preAggregationName);
  }

  get shouldReuseParams() {
    return false;
  }

  /**
   * Returns a dictionary mapping each preagregation to its corresponding query fragment.
   * @returns {Record<string, Array<string>>}
   */
  buildLambdaQuery() {
    const preAggForQuery = this.preAggregations.findPreAggregationForQuery();
    const result = {};
    if (preAggForQuery && preAggForQuery.preAggregation.unionWithSourceData) {
      const lambdaPreAgg = preAggForQuery.referencedPreAggregations[preAggForQuery.referencedPreAggregations.length - 1];
      // TODO(cristipp) Use source query instead of preaggregation references.
      const references = this.cubeEvaluator.evaluatePreAggregationReferences(lambdaPreAgg.cube, lambdaPreAgg.preAggregation);
      const lambdaQuery = this.newSubQuery(
        {
          measures: references.measures,
          dimensions: references.dimensions,
          timeDimensions: references.timeDimensions,
          filters: [
            ...this.options.filters ?? [],
            references.timeDimensions.length > 0
              ? {
                member: references.timeDimensions[0].dimension,
                operator: 'afterDate',
                values: [FROM_PARTITION_RANGE]
              }
              : [],
          ],
          segments: this.options.segments,
          order: [],
          limit: undefined,
          offset: undefined,
          rowLimit: MAX_SOURCE_ROW_LIMIT,
          preAggregationQuery: true,
        }
      );
      const sqlAndParams = lambdaQuery.buildSqlAndParams();
      const cacheKeyQueries = this.evaluateSymbolSqlWithContext(
        () => this.cacheKeyQueries(),
        { preAggregationQuery: true }
      );
      result[this.preAggregations.preAggregationId(lambdaPreAgg)] = { sqlAndParams, cacheKeyQueries };
    }
    return result;
  }

  externalQuery() {
    const ExternalQuery = this.externalQueryClass;
    return new ExternalQuery(this.compilers, {
      ...this.options,
      externalQueryClass: null
    });
  }

  runningTotalDateJoinCondition() {
    return this.timeDimensions
      .map(
        d => [
          d,
          (_dateFrom, dateTo, dateField, dimensionDateFrom, _dimensionDateTo) => `${dateField} >= ${dimensionDateFrom} AND ${dateField} <= ${dateTo}`
        ]
      );
  }

  rollingWindowToDateJoinCondition(granularity) {
    return Object.values(
      this.timeDimensions.reduce((acc, td) => {
        const key = td.dimension;

        if (!acc[key]) {
          acc[key] = td;
        }

        if (!acc[key].granularity && td.granularity) {
          acc[key] = td;
        }

        return acc;
      }, {})
    ).map(
      d => [
        d,
        (dateFrom, dateTo, dateField, _dimensionDateFrom, _dimensionDateTo, _isFromStartToEnd) => `${dateField} >= ${this.timeGroupedColumn(granularity, dateFrom)} AND ${dateField} <= ${dateTo}`
      ]
    );
  }

  rollingWindowDateJoinCondition(trailingInterval, leadingInterval, offset) {
    offset = offset || 'end';
    return Object.values(
      this.timeDimensions.reduce((acc, td) => {
        const key = td.dimension;

        if (!acc[key]) {
          acc[key] = td;
        }

        if (!acc[key].granularity && td.granularity) {
          acc[key] = td;
        }

        return acc;
      }, {})
    )
      .map(
        d => [d, (dateFrom, dateTo, dateField, _dimensionDateFrom, _dimensionDateTo, isFromStartToEnd) => {
          // dateFrom based window
          const conditions = [];
          if (trailingInterval !== 'unbounded') {
            const startDate = isFromStartToEnd || offset === 'start' ? dateFrom : dateTo;
            const trailingStart = trailingInterval ? this.subtractInterval(startDate, trailingInterval) : startDate;
            const sign = offset === 'start' ? '>=' : '>';
            conditions.push(`${dateField} ${sign} ${trailingStart}`);
          }
          if (leadingInterval !== 'unbounded') {
            const endDate = isFromStartToEnd || offset === 'end' ? dateTo : dateFrom;
            const leadingEnd = leadingInterval ? this.addInterval(endDate, leadingInterval) : endDate;
            const sign = offset === 'end' ? '<=' : '<';
            conditions.push(`${dateField} ${sign} ${leadingEnd}`);
          }
          return conditions.length ? conditions.join(' AND ') : '1 = 1';
        }]
      );
  }

  /**
   * @param {string} date
   * @param {string} interval
   * @returns {string}
   */
  subtractInterval(date, interval) {
    const intervalStr = this.intervalString(interval);
    return `${date} - interval ${intervalStr}`;
  }

  /**
   * @param {string} date
   * @param {string} interval
   * @returns {string}
   */
  addInterval(date, interval) {
    const intervalStr = this.intervalString(interval);
    return `${date} + interval ${intervalStr}`;
  }

  // For use in Tesseract
  supportGeneratedSeriesForCustomTd() {
    return false;
  }

  /**
   * @param {string} interval
   * @returns {string}
   */
  intervalString(interval) {
    return `'${interval}'`;
  }

  /**
   * @param {string} timestamp
   * @param {string} interval
   * @returns {string}
   */
  addTimestampInterval(timestamp, interval) {
    return this.addInterval(timestamp, interval);
  }

  /**
   * @param {string} timestamp
   * @param {string} interval
   * @returns {string}
   */
  subtractTimestampInterval(timestamp, interval) {
    return this.subtractInterval(timestamp, interval);
  }

  cumulativeMeasures() {
    return this.measures.filter(m => m.isCumulative());
  }

  isRolling() {
    return !!this.measures.find(m => m.isRolling()); // TODO
  }

  simpleQuery() {
    // eslint-disable-next-line prefer-template
    const inlineWhereConditions = [];
    const commonQuery = this.rewriteInlineWhere(() => this.commonQuery(), inlineWhereConditions);
    if (this.multiStageQuery) {
      return `${commonQuery} ${this.baseWhere(this.allFilters.concat(inlineWhereConditions))}`;
    }
    return `${commonQuery} ${this.baseWhere(this.allFilters.concat(inlineWhereConditions))}` +
      this.groupByClause() +
      this.baseHaving(this.measureFilters) +
      this.orderBy() +
      this.groupByDimensionLimit();
  }

  /**
   * Returns SQL query string.
   * @returns {string}
   */
  fullKeyQueryAggregate() {
    if (this.from) {
      return this.simpleQuery();
    }
    const {
      multipliedMeasures,
      regularMeasures,
      cumulativeMeasures,
      withQueries,
      multiStageMembers,
    } = this.fullKeyQueryAggregateMeasures();

    if (!multipliedMeasures.length && !cumulativeMeasures.length && !multiStageMembers.length) {
      return this.simpleQuery();
    }

    const renderedWithQueries = withQueries.map(q => this.renderWithQuery(q));

    let toJoin;
    if (this.options.preAggregationQuery) {
      const allRegular = regularMeasures.concat(
        cumulativeMeasures
          .map(
            ([multiplied, measure]) => (multiplied ? null : measure)
          )
          .filter(m => !!m)
      );
      const allMultiplied = multipliedMeasures.concat(
        cumulativeMeasures
          .map(
            ([multiplied, measure]) => (multiplied ? measure : null)
          )
          .filter(m => !!m)
      );
      toJoin = (allRegular.length ? [
        this.withCubeAliasPrefix(
          'main',
          () => this.regularMeasuresSubQuery(allRegular),
        )
      ] : [])
        .concat(
          R.pipe(
            R.groupBy(m => m.cube().name),
            R.toPairs,
            R.map(
              ([keyCubeName, measures]) => this.withCubeAliasPrefix(
                `${keyCubeName}_key`,
                () => this.aggregateSubQuery(keyCubeName, measures),
              )
            )
          )(allMultiplied)
        );
    } else {
      toJoin =
        (regularMeasures.length ? [
          this.withCubeAliasPrefix(
            'main',
            () => this.regularMeasuresSubQuery(regularMeasures),
          ),
        ] : [])
          .concat(
            R.pipe(
              R.groupBy(m => m.cube().name),
              R.toPairs,
              R.map(
                ([keyCubeName, measures]) => this
                  .withCubeAliasPrefix(
                    `${this.aliasName(keyCubeName)}_key`,
                    () => this.aggregateSubQuery(
                      keyCubeName,
                      measures,
                    )
                  )
              )
            )(multipliedMeasures)
          ).concat(
            R.map(
              ([multiplied, measure]) => this.withCubeAliasPrefix(
                `${this.aliasName(measure.measure.replace('.', '_'))
                }_cumulative`,
                () => this.overTimeSeriesQuery(
                  multiplied
                    ? (measures, filters) => this.aggregateSubQuery(
                      measures[0].cube().name,
                      measures,
                      filters,
                    )
                    : this.regularMeasuresSubQuery.bind(this),
                  measure,
                  false,
                ),
              )
            )(cumulativeMeasures)
            // TODO SELECT *
          ).concat(multiStageMembers.map(m => `SELECT * FROM ${m.alias}`));
    }

    // Move regular measures to multiplied ones if there are same
    // cubes to calculate. Most of the time it'll be much faster to
    // calculate as there will be only single scan per cube.
    if (
      regularMeasures.length &&
      multipliedMeasures.length &&
      !cumulativeMeasures.length
    ) {
      const cubeNames = R.pipe(
        R.map(m => m.cube().name),
        R.uniq,
        R.sortBy(R.identity),
      );
      const regularMeasuresCubes = cubeNames(regularMeasures);
      const multipliedMeasuresCubes = cubeNames(multipliedMeasures);
      if (R.equals(regularMeasuresCubes, multipliedMeasuresCubes)) {
        const measuresList = regularMeasures.concat(multipliedMeasures);
        // We need to use original measures sorting to avoid problems
        // with the query order.
        measuresList.sort((m1, m2) => {
          let i1;
          let i2;
          this.measures.forEach((m, i) => {
            if (m.measure === m1.measure) { i1 = i; }
            if (m.measure === m2.measure) { i2 = i; }
          });
          return i1 - i2;
        });
        toJoin = R.pipe(
          R.groupBy(m => m.cube().name),
          R.toPairs,
          R.map(
            ([keyCubeName, measures]) => this.withCubeAliasPrefix(
              `${keyCubeName}_key`,
              () => this.aggregateSubQuery(keyCubeName, measures),
            )
          )
        )(measuresList);
      }
    }

    const multiStageMeasures = R.flatten(multiStageMembers.map(m => m.measures)).map(m => this.newMeasure(m));

    return this.withQueries(this.joinFullKeyQueryAggregate(
      // TODO separate param?
      multipliedMeasures.concat(multiStageMeasures),
      regularMeasures,
      cumulativeMeasures,
      toJoin,
    ), renderedWithQueries);
  }

  joinFullKeyQueryAggregate(
    multipliedMeasures,
    regularMeasures,
    cumulativeMeasures,
    toJoin,
  ) {
    return this.outerMeasuresJoinFullKeyQueryAggregate(
      multipliedMeasures.concat(regularMeasures).concat(cumulativeMeasures.map(([multiplied, measure]) => measure)),
      this.measures,
      toJoin
    );
  }

  outerMeasuresJoinFullKeyQueryAggregate(innerMembers, outerMembers, toJoin) {
    const renderedReferenceContext = {
      renderedReference: R.pipe(
        R.map(m => {
          const member = m.measure ? m.measure : m.dimension;
          const memberPath = typeof member === 'string'
            ? member
            : this.cubeEvaluator.pathFromArray([m.measure?.originalCubeName ?? m.expressionCubeName, m.expressionName]);
          return [memberPath, m.aliasName()];
        }),
        R.fromPairs,
      )(innerMembers),
    };

    const join = R.drop(1, toJoin)
      .map(
        (q, i) => (this.dimensionAliasNames().length ?
          `INNER JOIN ${this.wrapInParenthesis((q))} as q_${i + 1} ON ${this.dimensionsJoinCondition(`q_${i}`, `q_${i + 1}`)}` :
          `, ${this.wrapInParenthesis(q)} as q_${i + 1}`),
      ).join('\n');

    const columnsToSelect = this.evaluateSymbolSqlWithContext(
      () => this.dimensionColumns('q_0').concat(outerMembers.map(m => m.selectColumns())).join(', '),
      renderedReferenceContext,
    );

    const queryHasNoRemapping = this.evaluateSymbolSqlWithContext(
      () => this.dimensionsForSelect().concat(outerMembers).every(r => r.hasNoRemapping()),
      renderedReferenceContext,
    );

    const havingFilters = this.evaluateSymbolSqlWithContext(
      () => this.baseWhere(this.measureFilters),
      renderedReferenceContext,
    );

    // TODO all having filters should be pushed down
    // subQuery dimensions can introduce projection remapping
    if (
      toJoin.length === 1 &&
      this.measureFilters.length === 0 &&
      outerMembers.filter(m => m.expression).length === 0 &&
      queryHasNoRemapping
    ) {
      return `${toJoin[0].replace(/^SELECT/, `SELECT ${this.topLimit()}`)} ${this.orderBy()}${this.groupByDimensionLimit()}`;
    }

    return `SELECT ${this.topLimit()}${columnsToSelect} FROM ${this.wrapInParenthesis(toJoin[0])} as q_0 ${join}${havingFilters}${this.orderBy()}${this.groupByDimensionLimit()}`;
  }

  wrapInParenthesis(select) {
    return select.trim().match(/^[a-zA-Z0-9_\-`".*]+$/i) ? select : `(${select})`;
  }

  withQueries(select, withQueries) {
    if (!withQueries || !withQueries.length) {
      return select;
    }
    // TODO escape alias
    return `WITH\n${withQueries.map(q => `${q.alias} AS (${q.query})`).join(',\n')}\n${select}`;
  }

  fullKeyQueryAggregateMeasures(context) {
    const measureToHierarchy = this.collectRootMeasureToHierarchy(context);
    const allMemberChildren = this.collectAllMemberChildren(context);
    const memberToIsMultiStage = this.collectAllMultiStageMembers(allMemberChildren);

    const hasMultiStageMembers = (m) => {
      if (memberToIsMultiStage[m]) {
        return true;
      }
      return allMemberChildren[m]?.some(c => hasMultiStageMembers(c)) || false;
    };

    const measuresToRender = (multiplied, cumulative) => R.pipe(
      R.values,
      R.flatten,
      R.filter(
        m => m.multiplied === multiplied && this.newMeasure(m.measure).isCumulative() === cumulative && !hasMultiStageMembers(m.measure)
      ),
      R.map(m => m.measure),
      R.uniq,
      R.map(m => this.newMeasure(m))
    );

    const multipliedMeasures = measuresToRender(true, false)(measureToHierarchy);
    const regularMeasures = measuresToRender(false, false)(measureToHierarchy);
    const cumulativeMeasures =
      R.pipe(
        R.map(multiplied => R.xprod([multiplied], measuresToRender(multiplied, true)(measureToHierarchy))),
        R.unnest
      )([false, true]);
    const withQueries = [];
    const multiStageMembers = R.uniq(
      this.allMembersConcat(false)
        // TODO boolean logic filter support
        .reduce((acc, m) => {
          if (m.isMemberExpression) {
            let refMemberPath;
            this.evaluateSql(m.cube().name, m.definition().sql, {
              sqlResolveFn: (_symbol, cube, prop) => {
                const path = this.cubeEvaluator.pathFromArray([cube, prop]);
                refMemberPath = path;
                // We don't need real SQL here, so just returning something.
                return path;
              }
            });

            if (hasMultiStageMembers(refMemberPath)) {
              acc.push(refMemberPath);
            }
          } else if (m.expressionPath && hasMultiStageMembers(m.expressionPath())) {
            acc.push(m.expressionPath());
          }

          return acc;
        }, [])
    ).map(m => this.multiStageWithQueries(
      m,
      {
        dimensions: this.dimensions.map(d => d.dimension),
        multiStageDimensions: this.dimensions.map(d => d.dimension),
        // TODO accessing timeDimensions directly from options might miss some processing logic
        timeDimensions: this.options.timeDimensions || [],
        multiStageTimeDimensions: (this.options.timeDimensions || []).filter(td => !!td.granularity),
        // TODO accessing filters directly from options might miss some processing logic
        filters: this.options.filters || [],
        segments: this.options.segments || [],
      },
      allMemberChildren,
      withQueries
    ));
    const usedWithQueries = {};
    multiStageMembers.forEach(m => this.collectUsedWithQueries(usedWithQueries, m));

    return {
      multipliedMeasures,
      regularMeasures,
      cumulativeMeasures,
      multiStageMembers,
      withQueries: withQueries.filter(q => usedWithQueries[q.alias])
    };
  }

  collectAllMemberChildren(context) {
    return this.collectFromMembers(
      false,
      (fn) => {
        const memberChildren = {};
        this.evaluateSymbolSqlWithContext(
          fn,
          { ...context, memberChildren },
        );
        return memberChildren;
      },
      context ? ['collectAllMemberChildren', JSON.stringify(context)] : 'collectAllMemberChildren',
    ).reduce((a, b) => ({ ...a, ...b }), {});
  }

  collectAllMultiStageMembers(allMemberChildren) {
    const allMembers = R.uniq(R.flatten(Object.keys(allMemberChildren).map(k => [k].concat(allMemberChildren[k]))));
    return R.fromPairs(allMembers.map(m => {
      // When `m` is coming from `collectAllMemberChildren`, it can contain `granularities.customGranularityName` in path
      // And it would mess up with join hints detection
      const trimmedPath = this
        .cubeEvaluator
        .parsePathAnyType(m)
        .slice(0, 2)
        .join('.');
      return [m, this.memberInstanceByPath(trimmedPath).isMultiStage()];
    }));
  }

  memberInstanceByPath(m) {
    let member;
    if (!member && this.cubeEvaluator.isMeasure(m)) {
      member = this.newMeasure(m);
    }
    if (!member && this.cubeEvaluator.isDimension(m)) {
      member = this.newDimension(m);
    }
    if (!member && this.cubeEvaluator.isSegment(m)) {
      member = this.newSegment(m);
    }
    if (!member) {
      throw new Error(`Can't resolve '${m}'`);
    }
    return member;
  }

  multiStageWithQueries(member, queryContext, memberChildren, withQueries) {
    // TODO calculate based on remove_filter in future
    const wouldNodeApplyFilters = !memberChildren[member];
    let memberFrom = memberChildren[member]
      ?.map(child => this.multiStageWithQueries(child, this.childrenMultiStageContext(member, queryContext), memberChildren, withQueries));
    const unionFromDimensions = memberFrom ? R.uniq(R.flatten(memberFrom.map(f => f.dimensions))) : queryContext.dimensions;
    const unionDimensionsContext = { ...queryContext, dimensions: unionFromDimensions.filter(d => !this.newDimension(d).isMultiStage()) };
    // TODO is calling multiStageWithQueries twice optimal?
    memberFrom = memberChildren[member] &&
      R.uniqBy(
        f => f.alias,
        memberChildren[member].map(child => this.multiStageWithQueries(child, this.childrenMultiStageContext(member, unionDimensionsContext), memberChildren, withQueries))
      );
    const selfContext = this.selfMultiStageContext(member, queryContext, wouldNodeApplyFilters);
    const subQuery = {
      ...selfContext,
      ...(this.cubeEvaluator.isMeasure(member) ? { measures: [member] } : { measures: [], dimensions: R.uniq(selfContext.dimensions.concat(member)) }),
      memberFrom,
    };

    const foundWith = withQueries.find(({ alias, ...q }) => R.equals(subQuery, q));

    if (foundWith) {
      return foundWith;
    }

    subQuery.alias = `cte_${withQueries.length}`;

    withQueries.push(subQuery);

    return subQuery;
  }

  collectUsedWithQueries(usedQueries, member) {
    usedQueries[member.alias] = true;
    member.memberFrom?.forEach(m => this.collectUsedWithQueries(usedQueries, m));
  }

  childrenMultiStageContext(memberPath, queryContext) {
    let member;
    if (this.cubeEvaluator.isMeasure(memberPath)) {
      member = this.newMeasure(memberPath);
    } else if (this.cubeEvaluator.isDimension(memberPath)) {
      member = this.newDimension(memberPath);
    }
    const memberDef = member.definition();
    // TODO can addGroupBy replaced by something else?
    if (memberDef.addGroupByReferences) {
      const dims = memberDef.addGroupByReferences.reduce((acc, cur) => {
        const pathArr = cur.split('.');
        // addGroupBy may include time dimension with granularity
        // But we don't need it as time dimension
        if (pathArr.length > 2) {
          pathArr.splice(2, 0, 'granularities');
          acc.push(pathArr.join('.'));
        } else {
          acc.push(cur);
        }
        return acc;
      }, []);
      queryContext = {
        ...queryContext,
        dimensions: R.uniq(queryContext.dimensions.concat(dims)),
      };
    }
    if (memberDef.timeShiftReferences?.length) {
      let { commonTimeShift } = queryContext;
      const timeShifts = queryContext.timeShifts || {};
      const memberOfCube = !this.cubeEvaluator.cubeFromPath(memberPath).isView;

      if (memberDef.timeShiftReferences.length === 1 && !memberDef.timeShiftReferences[0].timeDimension) {
        const timeShift = memberDef.timeShiftReferences[0];
        // We avoid view's timeshift evaluation as there will be another round of underlying cube's member evaluation
        if (memberOfCube) {
          commonTimeShift = timeShift.type === 'next' ? this.negateInterval(timeShift.interval) : timeShift.interval;
        }
      } else if (memberOfCube) {
        // We avoid view's timeshift evaluation as there will be another round of underlying cube's member evaluation
        memberDef.timeShiftReferences.forEach((r) => {
          timeShifts[r.timeDimension] = r.type === 'next' ? this.negateInterval(r.interval) : r.interval;
        });
      }

      queryContext = {
        ...queryContext,
        commonTimeShift,
        timeShifts,
      };
    }
    queryContext = {
      ...queryContext,
      // TODO can't remove filters from OR expression
      filters: this.keepFilters(queryContext.filters, filterMember => filterMember !== memberPath),
    };
    return queryContext;
  }

  selfMultiStageContext(memberPath, queryContext, wouldNodeApplyFilters) {
    let member;
    if (this.cubeEvaluator.isMeasure(memberPath)) {
      member = this.newMeasure(memberPath);
    } else if (this.cubeEvaluator.isDimension(memberPath)) {
      member = this.newDimension(memberPath);
      // TODO is it right place to replace context?
      // if (member.definition().type === 'rank') {
      //   queryContext = unionDimensionsContext;
      // }
    }
    const memberDef = member.definition();
    if (memberDef.reduceByReferences) {
      queryContext = {
        ...queryContext,
        multiStageDimensions: R.difference(queryContext.multiStageDimensions, memberDef.reduceByReferences),
        multiStageTimeDimensions: queryContext.multiStageTimeDimensions.filter(td => memberDef.reduceByReferences.indexOf(td.dimension) === -1),
        // dimensions: R.uniq(queryContext.dimensions.concat(memberDef.reduceByReferences))
      };
    }
    if (memberDef.groupByReferences) {
      queryContext = {
        ...queryContext,
        multiStageDimensions: R.intersection(queryContext.multiStageDimensions, memberDef.groupByReferences),
        multiStageTimeDimensions: queryContext.multiStageTimeDimensions.filter(td => memberDef.groupByReferences.indexOf(td.dimension) !== -1),
      };
    }
    if (!wouldNodeApplyFilters) {
      queryContext = {
        ...queryContext,
        // TODO make it same way as keepFilters
        timeDimensions: queryContext.timeDimensions.map(td => ({ ...td, dateRange: undefined })),
        // TODO keep segments related to this multistage (if applicable)
        segments: [],
        filters: this.keepFilters(queryContext.filters, filterMember => filterMember === memberPath),
      };
    } else {
      queryContext = {
        ...queryContext,
        // TODO remove not related segments
        // segments: queryContext.segments,
        filters: this.keepFilters(queryContext.filters, filterMember => !this.memberInstanceByPath(filterMember).isMultiStage()),
      };
    }
    return queryContext;
  }

  renderWithQuery(withQuery) {
    const fromMeasures = withQuery.memberFrom && R.uniq(R.flatten(withQuery.memberFrom.map(f => f.measures)));
    // TODO get rid of this multiStage filter
    const fromDimensions = withQuery.memberFrom && R.uniq(R.flatten(withQuery.memberFrom.map(f => f.dimensions)));
    const fromTimeDimensions = withQuery.memberFrom && R.uniq(R.flatten(withQuery.memberFrom.map(f => (f.timeDimensions || []).map(td => ({ ...td, dateRange: undefined })))));
    const renderedReferenceContext = {
      renderedReference: withQuery.memberFrom && R.fromPairs(
        R.unnest(withQuery.memberFrom.map(from => from.measures.map(m => {
          const measure = this.newMeasure(m);
          return [m, measure.aliasName()];
        }).concat(from.dimensions.map(m => {
          const member = this.newDimension(m);
          // In case of request coming from the SQL API, member could be expression-based
          const mPath = typeof m === 'string' ? m : this.cubeEvaluator.pathFromArray([m.cubeName, m.name]);
          return [mPath, member.aliasName()];
        })).concat(from.timeDimensions.map(m => {
          const member = this.newTimeDimension(m);
          return member.granularity ? [`${member.dimension}.${member.granularity}`, member.aliasName()] : [];
        }))))
      ),
      commonTimeShift: withQuery.commonTimeShift,
      timeShifts: withQuery.timeShifts,
    };

    const fromSubQuery = fromMeasures && this.newSubQuery({
      measures: fromMeasures,
      // TODO get rid of this multiStage filter
      dimensions: fromDimensions, // .filter(d => !this.newDimension(d).isMultiStage()),
      timeDimensions: fromTimeDimensions,
      multiStageDimensions: withQuery.multiStageDimensions,
      multiStageTimeDimensions: withQuery.multiStageTimeDimensions,
      filters: withQuery.filters,
      // TODO do we need it?
      multiStageQuery: true, // !!fromDimensions.find(d => this.newDimension(d).isMultiStage())
      disableExternalPreAggregations: true,
    });

    const measures = fromSubQuery && fromMeasures.map(m => fromSubQuery.newMeasure(m));
    // TODO get rid of this multiStage filter
    const multiStageDimensions = fromSubQuery && fromDimensions.map(m => fromSubQuery.newDimension(m)).filter(d => d.isMultiStage());
    const multiStageTimeDimensions = fromSubQuery && fromTimeDimensions.map(m => fromSubQuery.newTimeDimension(m)).filter(d => d.isMultiStage());
    // TODO not working yet
    const membersToSelect = measures?.concat(multiStageDimensions).concat(multiStageTimeDimensions);
    const select = fromSubQuery && fromSubQuery.outerMeasuresJoinFullKeyQueryAggregate(membersToSelect, membersToSelect, withQuery.memberFrom.map(f => f.alias));
    const fromSql = select && this.wrapInParenthesis(select);

    const subQueryOptions = {
      measures: withQuery.measures,
      dimensions: withQuery.dimensions,
      timeDimensions: withQuery.timeDimensions,
      multiStageDimensions: withQuery.multiStageDimensions,
      multiStageTimeDimensions: withQuery.multiStageTimeDimensions,
      filters: withQuery.filters,
      segments: withQuery.segments,
      from: fromSql && {
        sql: fromSql,
        alias: `${withQuery.alias}_join`,
      },
      // TODO condition should something else instead of rank
      multiStageQuery: !!withQuery.measures.find(d => {
        const { type } = this.newMeasure(d).definition();
        return type === 'rank' || CubeSymbols.isCalculatedMeasureType(type);
      }),
      disableExternalPreAggregations: true,
    };
    const subQuery = this.newSubQuery(subQueryOptions);

    if (!subQuery.from) {
      const allSubQueryMembers = R.flatten(subQuery.collectFromMembers(false, subQuery.collectMemberNamesFor.bind(subQuery), 'collectMemberNamesFor'));
      const multiStageMember = allSubQueryMembers.find(m => this.memberInstanceByPath(m).isMultiStage());
      if (multiStageMember) {
        throw new Error(`Multi stage member '${multiStageMember}' lacks FROM clause in sub query: ${JSON.stringify(subQueryOptions)}`);
      }
    }

    return {
      query: subQuery.evaluateSymbolSqlWithContext(
        () => subQuery.buildParamAnnotatedSql(),
        renderedReferenceContext,
      ),
      alias: withQuery.alias
    };
  }

  dimensionsJoinCondition(leftAlias, rightAlias) {
    const dimensionAliases = this.dimensionAliasNames();
    if (!dimensionAliases.length) {
      return '1 = 1';
    }
    return dimensionAliases
      .map(alias => `(${leftAlias}.${alias} = ${rightAlias}.${alias} OR (${leftAlias}.${alias} IS NULL AND ${rightAlias}.${alias} IS NULL))`)
      .join(' AND ');
  }

  baseWhere(filters) {
    const filterClause = filters.map(t => t.filterToWhere()).filter(R.identity).map(f => `(${f})`);
    return filterClause.length ? ` WHERE ${filterClause.join(' AND ')}` : '';
  }

  baseHaving(filters) {
    const filterClause = filters.map(t => t.filterToWhere()).filter(R.identity).map(f => `(${f})`);
    return filterClause.length ? ` HAVING ${filterClause.join(' AND ')}` : '';
  }

  timeStampInClientTz(dateParam) {
    return this.convertTz(dateParam);
  }

  granularityHierarchies() {
    return this.cacheValue(
      // If time dimension custom granularity in data model is defined without
      // timezone information they are treated in query timezone.
      // Because of that it's not possible to correctly precalculate
      // granularities hierarchies on startup as they are specific for each timezone.
      ['granularityHierarchies', this.timezone],
      () => R.reduce(
        (hierarchies, cube) => R.reduce(
          (acc, [tdName, td]) => {
            const dimensionKey = `${cube}.${tdName}`;

            // constructing standard granularities for time dimension
            const standardEntries = R.fromPairs(
              R.keys(standardGranularitiesParents).map(gr => [
                `${dimensionKey}.${gr}`,
                standardGranularitiesParents[gr],
              ]),
            );

            // If we have custom granularities in time dimension
            const customEntries = td.granularities
              ? R.fromPairs(
                R.keys(td.granularities).map(granularityName => {
                  const grObj = new Granularity(this, { dimension: dimensionKey, granularity: granularityName });
                  return [
                    `${dimensionKey}.${granularityName}`,
                    [granularityName, ...standardGranularitiesParents[grObj.minGranularity()]],
                  ];
                }),
              )
              : {};

            return { ...acc, ...standardEntries, ...customEntries };
          },
          hierarchies,
          R.toPairs(this.cubeEvaluator.timeDimensionsForCube(cube)),
        ),
        {},
        R.keys(this.cubeEvaluator.evaluatedCubes),
      ),
    );
  }

  granularityParentHierarchy(granularity) {
    return standardGranularitiesParents[granularity];
  }

  minGranularity(granularityA, granularityB) {
    if (!granularityA) {
      return granularityB;
    }
    if (!granularityB) {
      return granularityA;
    }
    if (granularityA === granularityB) {
      return granularityA;
    }
    const aHierarchy = R.reverse(this.granularityParentHierarchy(granularityA));
    const bHierarchy = R.reverse(this.granularityParentHierarchy(granularityB));
    let lastIndex = Math.max(
      aHierarchy.findIndex((g, i) => g !== bHierarchy[i]),
      bHierarchy.findIndex((g, i) => g !== aHierarchy[i])
    );
    if (lastIndex === -1 && aHierarchy.length === bHierarchy.length) {
      lastIndex = aHierarchy.length - 1;
    }
    if (lastIndex <= 0) {
      throw new Error(`Can't find common parent for '${granularityA}' and '${granularityB}'`);
    }
    return aHierarchy[lastIndex - 1];
  }

  overTimeSeriesQuery(baseQueryFn, cumulativeMeasure, fromRollup) {
    const dateJoinCondition = cumulativeMeasure.dateJoinCondition();
    const uniqDateJoinCondition = R.uniqBy(djc => djc[0].dimension, dateJoinCondition);
    const cumulativeMeasures = [cumulativeMeasure];
    if (!this.timeDimensions.find(d => d.granularity)) {
      const filters = this.segments
        .concat(this.filters)
        .concat(this.dateFromStartToEndConditionSql(
          // If the same time dimension is passed more than once, no need to build the same
          // filter condition again and again. Different granularities don't play role here,
          // as rollingWindow.granularity is used for filtering.
          uniqDateJoinCondition,
          fromRollup,
          false
        ));
      return baseQueryFn(cumulativeMeasures, filters, false);
    }

    if (this.timeDimensions.filter(d => !d.dateRange && d.granularity).length > 0) {
      throw new UserError('Time series queries without dateRange aren\'t supported');
    }

    // We can't do meaningful query if few time dimensions with different ranges passed,
    // it won't be possible to join them together without losing some rows.
    const rangedTimeDimensions = this.timeDimensions.filter(d => d.dateRange && d.granularity);
    const uniqTimeDimensionWithRanges = R.uniqBy(d => d.dateRange, rangedTimeDimensions);
    if (uniqTimeDimensionWithRanges.length > 1) {
      throw new Error('Can\'t build query for time dimensions with different date ranges');
    }

    // We need to generate time series table for the lowest granularity among all time dimensions
    const [dateSeriesDimension, dateSeriesGranularity] = this.timeDimensions.filter(d => d.granularity)
      .reduce(([prevDim, prevGran], d) => {
        const mg = this.minGranularity(prevGran, d.resolvedGranularity());
        if (mg === d.resolvedGranularity()) {
          return [d, mg];
        }
        return [prevDim, mg];
      }, [null, null]);

    const dateSeriesSql = this.dateSeriesSql(dateSeriesDimension);

    // If the same time dimension is passed more than once, no need to build the same
    // filter condition again and again. Different granularities don't play role here,
    // as rollingWindow.granularity is used for filtering.
    const filters = this.segments
      .concat(this.filters)
      .concat(this.dateFromStartToEndConditionSql(
        uniqDateJoinCondition,
        fromRollup,
        true
      ));
    const baseQuery = this.groupedUngroupedSelect(
      () => baseQueryFn(cumulativeMeasures, filters),
      cumulativeMeasure.shouldUngroupForCumulative(),
      !cumulativeMeasure.shouldUngroupForCumulative() && this.minGranularity(
        cumulativeMeasure.windowGranularity(),
        dateSeriesGranularity
      ) || undefined
    );
    const baseQueryAlias = this.cubeAlias('base');
    const dateJoinConditionSql =
      dateJoinCondition.map(
        ([d, f]) => f(
          // Time-series table is generated differently in different dialects,
          // but some dialects (like BigQuery) require strict date types and can not automatically convert
          // between date and timestamp for comparisons, at the same time, time dimensions are expected to be
          // timestamps, so we need to align types for join conditions/comparisons.
          // But we can't do it here, as it would break interval maths used in some types of
          // rolling window join conditions in some dialects (like Redshift), so we need to
          // do casts granularly in rolling window join conditions functions.
          `${d.dateSeriesAliasName()}.${this.escapeColumnName('date_from')}`,
          `${d.dateSeriesAliasName()}.${this.escapeColumnName('date_to')}`,
          `${baseQueryAlias}.${d.aliasName()}`,
          `'${d.dateFromFormatted()}'`,
          `'${d.dateToFormatted()}'`
        )
      ).join(' AND ');

    return this.overTimeSeriesSelect(
      cumulativeMeasures,
      dateSeriesSql,
      baseQuery,
      dateJoinConditionSql,
      baseQueryAlias,
      dateSeriesDimension.granularity,
    );
  }

  overTimeSeriesSelect(cumulativeMeasures, dateSeriesSql, baseQuery, dateJoinConditionSql, baseQueryAlias, dateSeriesGranularity) {
    const forSelect = this.overTimeSeriesForSelect(cumulativeMeasures, dateSeriesGranularity);
    return `SELECT ${forSelect} FROM ${dateSeriesSql}` +
      ` LEFT JOIN (${baseQuery}) ${this.asSyntaxJoin} ${baseQueryAlias} ON ${dateJoinConditionSql}` +
      this.groupByClause();
  }

  overTimeSeriesForSelect(cumulativeMeasures, dateSeriesGranularity) {
    return this.dimensions
      .map(s => s.cumulativeSelectColumns())
      .concat(this.timeDimensions.map(d => d.dateSeriesSelectColumn(null, dateSeriesGranularity)))
      .concat(cumulativeMeasures.map(s => s.cumulativeSelectColumns()))
      .filter(c => !!c)
      .join(', ');
  }

  /**
   * BigQuery has strict date type and can not automatically convert between date
   * and timestamp, so we override dateFromStartToEndConditionSql() in BigQuery Dialect
   * @protected
   */
  dateFromStartToEndConditionSql(dateJoinCondition, fromRollup, isFromStartToEnd) {
    return dateJoinCondition.map(
      // TODO Consider adding strict definitions of local and UTC time type
      ([d, f]) => ({
        filterToWhere: () => {
          const timeSeries = d.timeSeries();
          return f(
            isFromStartToEnd ?
              this.dateTimeCast(this.paramAllocator.allocateParam(timeSeries[0][0])) :
              `${this.timeStampInClientTz(d.dateFromParam())}`,
            isFromStartToEnd ?
              this.dateTimeCast(this.paramAllocator.allocateParam(timeSeries[timeSeries.length - 1][1])) :
              `${this.timeStampInClientTz(d.dateToParam())}`,
            `${fromRollup ? this.dimensionSql(d) : d.convertedToTz()}`,
            `${this.timeStampInClientTz(d.dateFromParam())}`,
            `${this.timeStampInClientTz(d.dateToParam())}`,
            isFromStartToEnd
          );
        }
      })
    );
  }

  /**
   * @param {import('./BaseTimeDimension').BaseTimeDimension} timeDimension
   * @return {string}
   */
  dateSeriesSql(timeDimension) {
    return `(${this.seriesSql(timeDimension)}) ${this.asSyntaxTable} ${timeDimension.dateSeriesAliasName()}`;
  }

  /**
   * BigQuery has strict date type and can not automatically convert between date
   * and timestamp, so we override seriesSql() in BigQuery Dialect
   * @param {import('./BaseTimeDimension').BaseTimeDimension} timeDimension
   * @return {string}
   */
  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `('${from}', '${to}')`
    );
    return `SELECT ${this.dateTimeCast('date_from')} as ${this.escapeColumnName('date_from')}, ${this.dateTimeCast('date_to')} as ${this.escapeColumnName('date_to')} FROM (VALUES ${values}) ${this.asSyntaxTable} dates (date_from, date_to)`;
  }

  /**
   * @param {import('./BaseDimension').BaseDimension|import('./BaseTimeDimension').BaseTimeDimension} timeDimension
   * @return {string}
   */
  timeStampParam(timeDimension) {
    return timeDimension.dateFieldType() === 'string' ? '?' : this.timeStampCast('?');
  }

  timeRangeFilter(dimensionSql, fromTimeStampParam, toTimeStampParam) {
    return `${dimensionSql} >= ${fromTimeStampParam} AND ${dimensionSql} <= ${toTimeStampParam}`;
  }

  timeNotInRangeFilter(dimensionSql, fromTimeStampParam, toTimeStampParam) {
    return `${dimensionSql} < ${fromTimeStampParam} OR ${dimensionSql} > ${toTimeStampParam}`;
  }

  beforeDateFilter(dimensionSql, timeStampParam) {
    return `${dimensionSql} < ${timeStampParam}`;
  }

  beforeOrOnDateFilter(dimensionSql, timeStampParam) {
    return `${dimensionSql} <= ${timeStampParam}`;
  }

  afterDateFilter(dimensionSql, timeStampParam) {
    return `${dimensionSql} > ${timeStampParam}`;
  }

  afterOrOnDateFilter(dimensionSql, timeStampParam) {
    return `${dimensionSql} >= ${timeStampParam}`;
  }

  timeStampCast(value) {
    return `${value}::timestamptz`;
  }

  dateTimeCast(value) {
    return `${value}::timestamp`;
  }

  /**
   * Converts the input interval (e.g. "2 years", "3 months", "5 days")
   * into a format compatible with the target SQL dialect.
   * Also returns the minimal time unit required (e.g. for use in DATEDIFF).
   *
   * Returns a tuple: (formatted interval, minimal time unit)
   */
  intervalAndMinimalTimeUnit(interval) {
    const minGranularity = this.diffTimeUnitForInterval(interval);
    return [interval, minGranularity];
  }

  commonQuery() {
    return `SELECT${this.topLimit()}
      ${this.baseSelect()}
    FROM
      ${this.query()}`;
  }

  dimensionOnlyMeasureToHierarchy(context, m) {
    const measureName = typeof m.measure === 'string' ? m.measure : `${m.measure.cubeName}.${m.measure.name}`;
    const memberNamesForMeasure = this.collectFrom(
      [m],
      this.collectMemberNamesFor.bind(this),
      context ? ['collectMemberNamesFor', JSON.stringify(context)] : 'collectMemberNamesFor',
      this.queryCache
    );
    const cubeNamesForMeasure = R.pipe(
      R.map(member => this.memberInstanceByPath(member)),
      // collectMemberNamesFor can return both view.dim and cube.dim
      R.filter(member => member.definition().ownedByCube),
      R.map(member => member.cube().name),
      // Single member expression can reference multiple dimensions from same cube
      R.uniq,
    )(
      memberNamesForMeasure
    );

    let cubeNameToAttach;
    switch (cubeNamesForMeasure.length) {
      case 0:
        // For zero reference measure there's nothing to derive info about measure from
        // So it assume that it's a regular measure, and it will be evaluated on top of join tree
        return [measureName, [{
          multiplied: false,
          measure: m.measure,
        }]];
      case 1:
        [cubeNameToAttach] = cubeNamesForMeasure;
        break;
      default:
        throw new Error(`Expected single cube for dimension-only measure ${measureName}, got ${cubeNamesForMeasure}`);
    }

    const multiplied = this.multipliedJoinRowResult(cubeNameToAttach) || false;

    const attachedMeasure = {
      ...m.measure,
      originalCubeName: m.measure.cubeName,
      cubeName: cubeNameToAttach
    };

    return [measureName, [{
      multiplied,
      measure: attachedMeasure,
    }]];
  }

  collectRootMeasureToHierarchy(context) {
    const notAddedMeasureFilters = R.flatten(this.measureFilters.map(f => f.getMembers()))
      .filter(f => R.none(m => m.measure === f.measure, this.measures));

    return R.fromPairs(this.measures.concat(notAddedMeasureFilters).map(m => {
      const collectedMeasures = this.collectFrom(
        [m],
        this.collectMultipliedMeasures(context),
        context ? ['collectMultipliedMeasures', JSON.stringify(context)] : 'collectMultipliedMeasures',
        this.queryCache
      );
      if (m.expressionName && !collectedMeasures.length && !m.isMemberExpression) {
        throw new UserError(`Subquery measure ${m.expressionName} should reference at least one member`);
      }

      if (collectedMeasures.length === 0 && m.isMemberExpression) {
        // `m` is member expression measure, but does not reference any other measure
        // Consider this dimensions-only measure. This can happen at least in 2 cases:
        // 1. Ad-hoc aggregation over dimension: SELECT MAX(dim) FROM cube
        // 2. Ungrouped query with SQL pushdown will render every column as measure: SELECT dim1 FROM cube WHERE LOWER(dim2) = 'foo';
        // Measures like this needs a special treatment to attach them to cube and decide if they are multiplied or not
        // This would return measure object in `measure`, not path
        // TODO return measure object for every measure
        return this.dimensionOnlyMeasureToHierarchy(context, m);
      }

      let measureKey;
      if (typeof m.measure === 'string') {
        measureKey = m.measure;
      } else if (m.isMemberExpression) {
        // TODO expressionName vs definition?
        measureKey = m.expressionName;
      } else {
        measureKey = `${m.measure.cubeName}.${m.measure.name}`;
      }
      return [measureKey, collectedMeasures];
    }));
  }

  query() {
    return this.from && this.joinSql([this.from]) || this.joinQuery(this.join, this.collectFromMembers(
      false,
      this.collectSubQueryDimensionsFor.bind(this),
      'collectSubQueryDimensionsFor'
    ));
  }

  /**
   *
   * @param {string} cube
   * @param {boolean} [isLeftJoinCondition]
   * @returns {[string, string, string?]}
   */
  rewriteInlineCubeSql(cube, isLeftJoinCondition) {
    const sql = this.cubeSql(cube);
    const cubeAlias = this.cubeAlias(cube);
    if (
      this.cubeEvaluator.cubeFromPath(cube).rewriteQueries
    ) {
      // TODO params independent sql caching
      const parser = this.queryCache.cache(['SqlParser', sql], () => new SqlParser(sql));
      if (parser.isSimpleAsteriskQuery()) {
        const conditions = parser.extractWhereConditions(cubeAlias);
        if (!isLeftJoinCondition && this.safeEvaluateSymbolContext().inlineWhereConditions) {
          this.safeEvaluateSymbolContext().inlineWhereConditions.push({ filterToWhere: () => conditions });
        }
        return [parser.extractTableFrom(), cubeAlias, conditions];
      } else {
        return [sql, cubeAlias];
      }
    } else {
      return [sql, cubeAlias];
    }
  }

  /**
   * @param {import('../compiler/JoinGraph').FinishedJoinTree} join
   * @param {Array<string>} subQueryDimensions
   * @returns {string}
   */
  joinQuery(join, subQueryDimensions) {
    const subQueryDimensionsByCube = R.groupBy(d => this.cubeEvaluator.cubeNameFromPath(d), subQueryDimensions);
    const joins = join.joins.flatMap(
      j => {
        const [cubeSql, cubeAlias, conditions] = this.rewriteInlineCubeSql(j.originalTo, true);
        return [{
          sql: cubeSql,
          alias: cubeAlias,
          on: `${this.evaluateSql(j.originalFrom, j.join.sql)}${conditions ? ` AND (${conditions})` : ''}`
          // TODO handle the case when sub query referenced by a foreign cube on other side of a join
        }].concat((subQueryDimensionsByCube[j.originalTo] || []).map(d => this.subQueryJoin(d)));
      }
    );

    const [cubeSql, cubeAlias] = this.rewriteInlineCubeSql(join.root);

    return this.joinSql([
      { sql: cubeSql, alias: cubeAlias },
      ...(subQueryDimensionsByCube[join.root] || []).map(d => this.subQueryJoin(d)),
      ...joins,
      ...this.customSubQueryJoins.map((customJoin) => this.customSubQueryJoin(customJoin)),
    ]);
  }

  /**
   * @param {JoinChain} toJoin
   * @returns {string}
   */
  joinSql(toJoin) {
    const [root, ...rest] = toJoin;
    const joins = rest.map(
      j => {
        const joinType = j.joinType ?? 'LEFT';
        return `${joinType} JOIN ${j.sql} ${this.asSyntaxJoin} ${j.alias} ON ${j.on}`;
      }
    );

    return [`${root.sql} ${this.asSyntaxJoin} ${root.alias}`, ...joins].join('\n');
  }

  /**
   *
   * @param {{sql: string, on: {cubeName: string, expression: Function}, joinType: 'LEFT' | 'INNER', alias: string}}
   *   customJoin
   * @returns {JoinItem}
   */
  customSubQueryJoin(customJoin) {
    const on = this.evaluateSql(customJoin.on.cubeName, customJoin.on.expression);

    return {
      sql: `(${customJoin.sql})`,
      alias: customJoin.alias,
      on,
      joinType: customJoin.joinType,
    };
  }

  /**
   *
   * @param {string} dimension
   * @returns {JoinItem}
   */
  subQueryJoin(dimension) {
    const { prefix, subQuery, cubeName } = this.subQueryDescription(dimension);
    const primaryKeys = this.cubeEvaluator.primaryKeys[cubeName];
    const subQueryAlias = this.escapeColumnName(this.aliasName(prefix));

    const { collectOriginalSqlPreAggregations } = this.safeEvaluateSymbolContext();
    const sql = subQuery.evaluateSymbolSqlWithContext(() => subQuery.buildParamAnnotatedSql(), {
      collectOriginalSqlPreAggregations
    });
    const onCondition = primaryKeys.map((pk) => `${subQueryAlias}.${this.newDimension(this.primaryKeyName(cubeName, pk)).aliasName()} = ${this.primaryKeySql(pk, cubeName)}`);

    return {
      sql: `(${sql})`,
      alias: subQueryAlias,
      on: onCondition.join(' AND ')
    };
  }

  get filtersWithoutSubQueries() {
    if (!this.filtersWithoutSubQueriesValue) {
      this.filtersWithoutSubQueriesValue = this.allFilters.filter(
        f => this.collectFrom([f], this.collectSubQueryDimensionsFor.bind(this), 'collectSubQueryDimensionsFor').length === 0
      );
    }
    return this.filtersWithoutSubQueriesValue;
  }

  /**
   *
   * @param {string} dimension
   * @returns {{ prefix: string, subQuery: this, cubeName: string }}
   */
  subQueryDescription(dimension) {
    const symbol = this.cubeEvaluator.dimensionByPath(dimension);
    const [cubeName, name] = this.cubeEvaluator.parsePath('dimensions', dimension);
    const prefix = this.subQueryName(cubeName, name);
    let filters;
    let segments;
    let timeDimensions;
    if (symbol.propagateFiltersToSubQuery) {
      filters = this.filtersWithoutSubQueries.filter(
        f => f instanceof BaseFilter && !(f instanceof BaseTimeDimension)
      ).map(f => ({
        dimension: f.dimension,
        operator: f.operator,
        values: f.values
      }));

      timeDimensions = this.filtersWithoutSubQueries.filter(
        f => f instanceof BaseTimeDimension
      ).map(f => ({
        dimension: f.dimension,
        dateRange: f.dateRange
      }));

      segments = this.filtersWithoutSubQueries.filter(
        f => f instanceof BaseSegment
      ).map(f => f.segment);
    }
    const subQuery = this.newSubQuery({
      cubeAliasPrefix: prefix,
      rowLimit: null,
      measures: [{
        expression: symbol.sql,
        cubeName,
        name
      }],
      dimensions: this.primaryKeyNames(cubeName),
      filters,
      segments,
      timeDimensions,
      order: {}
    });
    return { prefix, subQuery, cubeName };
  }

  /**
   *
   * @param {string} cubeName
   * @param {string} name
   * @returns {string}
   */
  subQueryName(cubeName, name) {
    return `${cubeName}_${name}_subquery`;
  }

  regularMeasuresSubQuery(measures, filters) {
    filters = filters || this.allFilters;

    const inlineWhereConditions = [];

    const query = this.rewriteInlineWhere(() => this.joinQuery(
      this.join,
      this.collectFrom(
        this.dimensionsForSelect().concat(measures).concat(this.allFilters),
        this.collectSubQueryDimensionsFor.bind(this),
        'collectSubQueryDimensionsFor'
      )
    ), inlineWhereConditions);

    return `SELECT ${this.selectAllDimensionsAndMeasures(measures)} FROM ${query
    } ${this.baseWhere(filters.concat(inlineWhereConditions))}` +
      (!this.safeEvaluateSymbolContext().ungrouped && this.groupByClause() || '');
  }

  /**
   * Returns SQL query for the "aggregating on top of sub-queries" uses cases.
   * @param {string} keyCubeName
   * @param {Array<BaseMeasure>} measures
   * @param {Array<BaseFilter>} filters
   * @returns {string}
   */
  aggregateSubQuery(keyCubeName, measures, filters) {
    filters = filters || this.allFilters;
    const primaryKeyDimensions = this.primaryKeyNames(keyCubeName).map((k) => this.newDimension(k));
    const shouldBuildJoinForMeasureSelect = this.checkShouldBuildJoinForMeasureSelect(measures, keyCubeName);

    let keyCubeSql;
    let keyCubeAlias;
    let keyCubeInlineLeftJoinConditions;
    const measureSubQueryDimensions = this.collectFrom(
      measures,
      this.collectSubQueryDimensionsFor.bind(this),
      'collectSubQueryDimensionsFor'
    );

    if (shouldBuildJoinForMeasureSelect) {
      const joinHints = this.collectJoinHintsFromMembers(measures);
      const measuresJoin = this.joinGraph.buildJoin(joinHints);
      if (measuresJoin.multiplicationFactor[keyCubeName]) {
        throw new UserError(
          `'${measures.map(m => m.measure).join(', ')}' reference cubes that lead to row multiplication.`
        );
      }
      keyCubeSql = `(${this.aggregateSubQueryMeasureJoin(keyCubeName, measures, measuresJoin, primaryKeyDimensions, measureSubQueryDimensions)})`;
      keyCubeAlias = this.cubeAlias(keyCubeName);
    } else {
      [keyCubeSql, keyCubeAlias, keyCubeInlineLeftJoinConditions] = this.rewriteInlineCubeSql(keyCubeName);
    }

    const measureSelectFn = () => measures.map(m => m.selectColumns());
    const selectedMeasures = shouldBuildJoinForMeasureSelect ? this.evaluateSymbolSqlWithContext(
      measureSelectFn,
      {
        ungroupedAliases: R.fromPairs(measures.map(m => [m.measure, m.aliasName()]))
      }
    ) : measureSelectFn();
    const columnsForSelect = this
      .dimensionColumns(this.escapeColumnName(QueryAlias.AGG_SUB_QUERY_KEYS))
      .concat(selectedMeasures)
      .filter(s => !!s)
      .join(', ');

    const primaryKeyJoinConditions = primaryKeyDimensions.map((pkd) => (
      `${this.escapeColumnName(QueryAlias.AGG_SUB_QUERY_KEYS)
      }.${pkd.aliasName()
      } = ${shouldBuildJoinForMeasureSelect
        ? `${this.cubeAlias(keyCubeName)}.${pkd.aliasName()}`
        : this.dimensionSql(pkd)
      }`
    )).join(' AND ');

    const subQueryJoins =
      shouldBuildJoinForMeasureSelect ? [] : measureSubQueryDimensions.map(d => this.subQueryJoin(d));
    const joinSql = this.joinSql([
      {
        sql: `(${this.keysQuery(primaryKeyDimensions, filters)})`,
        alias: this.escapeColumnName(QueryAlias.AGG_SUB_QUERY_KEYS),
      },
      {
        sql: keyCubeSql,
        alias: keyCubeAlias,
        on: `${primaryKeyJoinConditions}
             ${keyCubeInlineLeftJoinConditions ? ` AND (${keyCubeInlineLeftJoinConditions})` : ''}`,
      },
      ...subQueryJoins
    ]);
    return `SELECT ${columnsForSelect} FROM ${joinSql}` +
      (!this.safeEvaluateSymbolContext().ungrouped && this.aggregateSubQueryGroupByClause() || '');
  }

  /**
   * @param {Array<BaseMeasure>} measures
   * @param {string} keyCubeName
   * @returns {boolean}
   */
  checkShouldBuildJoinForMeasureSelect(measures, keyCubeName) {
    // When member expression references view, it would have to collect join hints from view
    // Consider join A->B, as many-to-one, so B is multiplied and A is not, and member expression like SUM(AB_view.dimB)
    // Both `collectCubeNamesFor` and `collectJoinHintsFor` would return too many cubes here
    // They both walk join hints, and gather every cube present there
    // For view we would get both A and B, because join hints would go from join tree root
    // Even though expression references only B, and should be OK to use it with B as keyCube
    // So this check would build new join tree from both A and B, B will be multiplied, and that would break check

    return measures.map(measure => {
      const memberNamesForMeasure = this.collectFrom(
        [measure],
        this.collectMemberNamesFor.bind(this),
        'collectMemberNamesFor',
      );

      const nonViewMembers = memberNamesForMeasure
        .map(member => this.memberInstanceByPath(member))
        .filter(member => member.definition().ownedByCube);

      const cubes = this.collectFrom(nonViewMembers, this.collectCubeNamesFor.bind(this), 'collectCubeNamesFor');
      // Not using `collectJoinHintsFromMembers([measure])` because it would collect too many join hints from view
      const joinHints = [
        measure.joinHint,
        ...this.collectJoinHintsFromMembers(nonViewMembers),
      ];
      if (R.any(cubeName => keyCubeName !== cubeName, cubes)) {
        const measuresJoin = this.joinGraph.buildJoin(joinHints);
        if (measuresJoin.multiplicationFactor[keyCubeName]) {
          const measureName = measure.isMemberExpression ? measure.expressionName : measure.measure;
          throw new UserError(
            `'${measureName}' references cubes that lead to row multiplication. Please rewrite it using sub query.`
          );
        }
        return true;
      }
      return false;
    }).reduce((a, b) => a || b);
  }

  aggregateSubQueryMeasureJoin(keyCubeName, measures, measuresJoin, primaryKeyDimensions, measureSubQueryDimensions) {
    return this.ungroupedMeasureSelect(() => this.withCubeAliasPrefix(`${keyCubeName}_measure_join`,
      () => {
        const columns = primaryKeyDimensions.map(p => p.selectColumns()).concat(measures.map(m => m.selectColumns()))
          .filter(s => !!s).join(', ');
        return `SELECT ${columns} FROM ${this.joinQuery(measuresJoin, measureSubQueryDimensions)}`;
      }));
  }

  groupedUngroupedSelect(select, ungrouped, granularityOverride) {
    return this.evaluateSymbolSqlWithContext(
      select,
      { ungrouped, granularityOverride, overTimeSeriesAggregate: true }
    );
  }

  ungroupedMeasureSelect(select) {
    return this.evaluateSymbolSqlWithContext(
      select,
      { ungrouped: true }
    );
  }

  keysQuery(primaryKeyDimensions, filters) {
    const inlineWhereConditions = [];
    const query = this.rewriteInlineWhere(() => this.joinQuery(
      this.join,
      this.collectFrom(
        this.keyDimensions(primaryKeyDimensions),
        this.collectSubQueryDimensionsFor.bind(this),
        'collectSubQueryDimensionsFor'
      )
    ), inlineWhereConditions);
    return `SELECT DISTINCT ${this.keysSelect(primaryKeyDimensions)} FROM ${query
    } ${this.baseWhere(filters.concat(inlineWhereConditions))}`;
  }

  keysSelect(primaryKeyDimensions) {
    return R.flatten(
      this.keyDimensions(primaryKeyDimensions)
        .map(s => s.selectColumns())
    ).filter(s => !!s).join(', ');
  }

  keyDimensions(primaryKeyDimensions) {
    // The same dimension with different granularities maybe requested, so it's not enough to filter only by dimension
    return R.uniqBy(
      (d) => {
        if (d.isMemberExpression) {
          return d.dimension.definition;
        }

        return `${d.dimension}${d.granularity ?? ''}`;
      },
      this.dimensionsForSelect()
        .concat(primaryKeyDimensions)
    );
  }

  /**
   * @param {string} cube
   */
  cubeSql(cube) {
    const foundPreAggregation = this.preAggregations.findPreAggregationToUseForCube(cube);
    if (foundPreAggregation &&
      (!this.options.preAggregationQuery || this.options.useOriginalSqlPreAggregationsInPreAggregation) &&
      !this.safeEvaluateSymbolContext().preAggregationQuery
    ) {
      if (this.safeEvaluateSymbolContext().collectOriginalSqlPreAggregations) {
        this.safeEvaluateSymbolContext().collectOriginalSqlPreAggregations.push(foundPreAggregation);
      }
      return this.preAggregations.originalSqlPreAggregationTable(foundPreAggregation);
    }

    const fromPath = this.cubeEvaluator.cubeFromPath(cube);
    if (fromPath.sqlTable) {
      return this.evaluateSql(cube, fromPath.sqlTable);
    }

    const evaluatedSql = this.evaluateSql(cube, fromPath.sql);
    const selectAsterisk = evaluatedSql.match(/^\s*select\s+\*\s+from\s+([a-zA-Z0-9_\-`".*]+)\s*$/i);
    if (selectAsterisk) {
      return selectAsterisk[1];
    }

    return `(${evaluatedSql})`;
  }

  traverseSymbol(s) {
    // TODO why not just evaluateSymbolSql for every branch?
    if (s.path()) {
      return [s.cube().name].concat(this.evaluateSymbolSql(s.path()[0], s.path()[1], s.definition()));
    } else if (s.patchedMeasure?.patchedFrom) {
      return [s.patchedMeasure.patchedFrom.cubeName].concat(this.evaluateSymbolSql(s.patchedMeasure.patchedFrom.cubeName, s.patchedMeasure.patchedFrom.name, s.definition()));
    } else {
      const res = this.evaluateSql(s.cube().name, s.definition().sql);
      if (s.isJoinCondition) {
        // In a join between Cube A and Cube B, sql() may reference members from other cubes.
        // These referenced cubes must be added as join hints before Cube B to ensure correct SQL generation.
        const targetCube = s.targetCubeName();
        let { joinHints } = this.safeEvaluateSymbolContext();
        joinHints = joinHints.filter(e => e !== targetCube);
        joinHints.push(targetCube);
        this.safeEvaluateSymbolContext().joinHints = joinHints;
      }
      return res;
    }
  }

  /**
   *
   * @returns {Array<string>}
   */
  collectCubeNames() {
    return this.collectFromMembers(
      false,
      this.collectCubeNamesFor.bind(this),
      'collectCubeNamesFor'
    );
  }

  /**
   *
   * @param {boolean} [excludeTimeDimensions=false]
   * @returns {Array<Array<string>>}
   */
  collectJoinHints(excludeTimeDimensions = false) {
    const membersToCollectFrom = [
      ...this.allMembersConcat(excludeTimeDimensions),
      ...this.joinMembersFromJoin(this.join),
      ...this.joinMembersFromCustomSubQuery(),
    ];

    return this.collectJoinHintsFromMembers(membersToCollectFrom);
  }

  joinMembersFromCustomSubQuery() {
    return this.customSubQueryJoins.map(j => {
      const res = {
        path: () => null,
        cube: () => this.cubeEvaluator.cubeFromPath(j.on.cubeName),
        definition: () => ({
          sql: j.on.expression,
          // TODO use actual type even though it isn't used right now
          type: 'number'
        }),
      };
      return {
        getMembers: () => [res],
      };
    });
  }

  joinMembersFromJoin(join) {
    return join ? join.joins.map(j => ({
      getMembers: () => [{
        path: () => null,
        cube: () => this.cubeEvaluator.cubeFromPath(j.originalFrom),
        definition: () => j.join,
        isJoinCondition: true,
        targetCubeName: () => j.originalTo,
      }]
    })) : [];
  }

  collectJoinHintsFromMembers(members) {
    return [
      ...members.map(m => m.joinHint).filter(h => h?.length > 0),
      ...this.collectFrom(members, this.collectJoinHintsFor.bind(this), 'collectJoinHintsFromMembers'),
    ];
  }

  /**
   * @template T
   * @param {boolean} excludeTimeDimensions
   * @param {(t: () => void) => T} fn
   * @param {string | Array<string>} methodName
   * @returns {T}
   */
  collectFromMembers(excludeTimeDimensions, fn, methodName) {
    const membersToCollectFrom = this.allMembersConcat(excludeTimeDimensions)
      .concat(this.join ? this.join.joins.map(j => ({
        getMembers: () => [{
          path: () => null,
          cube: () => this.cubeEvaluator.cubeFromPath(j.originalFrom),
          definition: () => j.join,
        }]
      })) : []);
    return this.collectFrom(membersToCollectFrom, fn, methodName);
  }

  /**
   *
   * @param {boolean} excludeTimeDimensions
   * @returns {Array<BaseMeasure | BaseDimension | BaseSegment>}
   */
  allMembersConcat(excludeTimeDimensions) {
    return this.measures
      .concat(this.dimensions)
      .concat(this.segments)
      .concat(this.filters)
      .concat(this.measureFilters)
      .concat(excludeTimeDimensions ? [] : this.timeDimensions);
  }

  /**
   * @template T
   * @param {Array<unknown>} membersToCollectFrom
   * @param {(t: () => void) => T} fn
   * @param {string | Array<string>} methodName
   * @param {unknown} [cache]
   * @returns {T}
   */
  collectFrom(membersToCollectFrom, fn, methodName, cache) {
    const methodCacheKey = Array.isArray(methodName) ? methodName : [methodName];
    return R.pipe(
      R.map(f => f.getMembers()),
      R.flatten,
      R.map(s => (
        (cache || this.compilerCache).cache(
          ['collectFrom'].concat(methodCacheKey).concat(
            s.path() ? [s.path().join('.')] : [s.cube().name, s.expression?.toString() || s.expressionName || s.definition().sql]
          ),
          () => fn(() => this.traverseSymbol(s))
        )
      )),
      R.unnest,
      R.uniq,
      R.filter(R.identity)
    )(
      membersToCollectFrom
    );
  }

  /**
   *
   * @param {() => void} fn
   * @returns {Array<string>}
   */
  collectSubQueryDimensionsFor(fn) {
    const context = { subQueryDimensions: [] };
    this.evaluateSymbolSqlWithContext(
      fn,
      context
    );
    return R.uniq(context.subQueryDimensions);
  }

  rewriteInlineWhere(fn, inlineWhereConditions) {
    const context = { inlineWhereConditions };
    return this.evaluateSymbolSqlWithContext(
      fn,
      context
    );
  }

  /**
   * Returns `GROUP BY` clause for the "aggregating on top of sub-queries" uses
   * cases. By the default returns the result of the `groupByClause` method.
   * @returns {string}
   */
  aggregateSubQueryGroupByClause() {
    return this.groupByClause();
  }

  /**
   * Returns `GROUP BY` clause for the basic uses cases.
   * @returns {string}
   */
  groupByClause() {
    if (this.ungrouped) {
      return '';
    }
    const dimensionColumns = this.dimensionColumns();
    if (!dimensionColumns.length) {
      return '';
    }
    const dimensionNames = dimensionColumns.map((c, i) => `${i + 1}`);
    return this.rollupGroupByClause(dimensionNames);
  }

  /**
   * XXX: String as return value is added because of HiveQuery.getFieldIndex() and DatabricksQuery.getFieldIndex()
   * @protected
   * @param {string} id member name in form of "cube.member[.granularity]"
   * @returns {number|string|null}
   */
  getFieldIndex(id) {
    const equalIgnoreCase = (a, b) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let index = -1;
    const path = id.split('.');

    // Granularity is specified
    if (path.length === 3) {
      const memberName = path.slice(0, 2).join('.');
      const granularity = path[2];

      index = this.timeDimensions
        // Not all time dimensions are used in select list, some are just filters,
        // but they exist in this.timeDimensions, so need to filter them out
        .filter(d => d.selectColumns())
        .findIndex(
          d => (
            (equalIgnoreCase(d.dimension, memberName) && (d.granularityObj?.granularity === granularity)) ||
            equalIgnoreCase(d.expressionName, memberName)
          )
        );

      if (index > -1) {
        return index + 1;
      }

      // TODO IT would be nice to log a warning that requested member wasn't found, but we don't have a logger here
      return null;
    }

    const dimensionsForSelect = this.dimensionsForSelect()
      // Not all time dimensions are used in select list, some are just filters,
      // but they exist in this.timeDimensions, so need to filter them out
      .filter(d => d.selectColumns());

    const found = findMinGranularityDimension(id, dimensionsForSelect);
    if (found?.index > -1) {
      return found.index + 1;
    }

    index = this.measures.findIndex(
      d => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id)
    );

    if (index > -1) {
      const dimensionsCount = this.dimensionColumns().length;
      return index + dimensionsCount + 1;
    }

    return null;
  }

  /**
   * @protected
   * @param {string} id member name in form of "cube.member[.granularity]"
   * @returns {null|string}
   */
  getFieldAlias(id) {
    const equalIgnoreCase = (a, b) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let field;

    const path = id.split('.');

    // Granularity is specified
    if (path.length === 3) {
      const memberName = path.slice(0, 2).join('.');
      const granularity = path[2];

      field = this.timeDimensions
        // Not all time dimensions are used in select list, some are just filters,
        // but they exist in this.timeDimensions, so need to filter them out
        .filter(d => d.selectColumns())
        .find(
          d => (
            (equalIgnoreCase(d.dimension, memberName) && (d.granularityObj?.granularity === granularity)) ||
            equalIgnoreCase(d.expressionName, memberName)
          )
        );

      if (field) {
        return field.aliasName();
      }

      return null;
    }

    const dimensionsForSelect = this.dimensionsForSelect()
      // Not all time dimensions are used in select list, some are just filters,
      // but they exist in this.timeDimensions, so need to filter them out
      .filter(d => d.selectColumns());

    const found = findMinGranularityDimension(id, dimensionsForSelect);

    if (found?.dimension) {
      return found.dimension.aliasName();
    }

    field = this.measures.find(
      (d) => equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id),
    );

    if (field) {
      return field.aliasName();
    }

    return null;
  }

  /**
   * @param {{ id: string, desc: boolean }} hash
   * @returns {string|null}
   */
  orderHashToString(hash) {
    if (!hash || !hash.id) {
      return null;
    }

    const fieldIndex = this.getFieldIndex(hash.id);

    if (fieldIndex === null) {
      return null;
    }

    const direction = hash.desc ? 'DESC' : 'ASC';
    return `${fieldIndex} ${direction}`;
  }

  orderBy() {
    if (R.isEmpty(this.order)) {
      return '';
    }

    const orderByString = R.pipe(
      R.map(this.orderHashToString),
      R.reject(R.isNil),
      R.join(', ')
    )(this.order);

    if (!orderByString) {
      return '';
    }

    return ` ORDER BY ${orderByString}`;
  }

  /**
   * Returns a complete list of the aliased dimensions, including time
   * dimensions.
   * @returns {Array<string>}
   */
  dimensionAliasNames() {
    return R.flatten(this.dimensionsForSelect().map(d => d.aliasName()).filter(d => !!d));
  }

  /**
   * Returns an array of column names correlated to the specified cube dimensions.
   * @param {string} cubeAlias
   * @returns {Array<string>}
   */
  dimensionColumns(cubeAlias) {
    return this.dimensionAliasNames().map(alias => `${cubeAlias && `${cubeAlias}.` || ''}${alias}`);
  }

  groupByDimensionLimit() {
    let limit = null;
    if (this.rowLimit !== null) {
      if (this.rowLimit === MAX_SOURCE_ROW_LIMIT) {
        limit = this.paramAllocator.allocateParam(MAX_SOURCE_ROW_LIMIT);
      } else if (typeof this.rowLimit === 'number') {
        limit = this.rowLimit;
      }
    }
    const offset = this.offset ? parseInt(this.offset, 10) : null;
    return this.limitOffsetClause(limit, offset);
  }

  /**
   * @protected
   * @param {Array<string>} dimensionNames
   * @returns {string}
   */
  rollupGroupByClause(dimensionNames) {
    if (this.ungrouped) {
      return '';
    }
    const dimensionColumns = this.dimensionColumns();
    if (!dimensionColumns.length) {
      return '';
    }

    const groupingSets = R.flatten(this.dimensionsForSelect().map(d => d.dimension).filter(d => !!d)).map(d => d.groupingSet);

    let result = ' GROUP BY ';

    dimensionColumns.forEach((c, i) => {
      const groupingSet = groupingSets[i];
      const comma = i > 0 ? ', ' : '';
      const prevId = i > 0 ? (groupingSets[i - 1] || { id: null }).id : null;
      const currId = (groupingSet || { id: null }).id;

      if (prevId !== null && currId !== prevId) {
        result += ')';
      }

      if ((prevId === null || currId !== prevId) && groupingSet != null) {
        if (groupingSet.groupType === 'Rollup') {
          result += `${comma}ROLLUP(`;
        } else if (groupingSet.groupType === 'Cube') {
          result += `${comma}CUBE(`;
        }
      } else {
        result += `${comma}`;
      }

      result += dimensionNames[i];
    });
    if (groupingSets[groupingSets.length - 1] != null) {
      result += ')';
    }

    return result;
  }

  /**
   * @protected
   * @param limit
   * @param offset
   * @returns {string}
   */
  limitOffsetClause(limit, offset) {
    const limitClause = limit != null ? ` LIMIT ${limit}` : '';
    const offsetClause = offset != null ? ` OFFSET ${offset}` : '';
    return `${limitClause}${offsetClause}`;
  }

  topLimit() {
    return '';
  }

  baseSelect() {
    return R.flatten(this.forSelect().map(s => s.selectColumns())).filter(s => !!s).join(', ');
  }

  selectAllDimensionsAndMeasures(measures) {
    return R.flatten(
      this.dimensionsForSelect().concat(measures).map(s => s.selectColumns())
    ).filter(s => !!s).join(', ');
  }

  /**
   * @returns {Array<BaseDimension|BaseMeasure>}
   */
  forSelect() {
    return this.dimensionsForSelect().concat(this.measures);
  }

  /**
   * Returns a complete list of the dimensions, including time dimensions.
   * @returns {(BaseDimension|BaseTimeDimension)[]}
   */
  dimensionsForSelect() {
    return this.dimensions.concat(this.timeDimensions);
  }

  dimensionSql(dimension) {
    return this.evaluateSymbolSql(dimension.path()[0], dimension.path()[1], dimension.dimensionDefinition());
  }

  segmentSql(segment) {
    return this.evaluateSymbolSql(segment.path()[0], segment.path()[1], segment.segmentDefinition());
  }

  measureSql(measure) {
    return this.evaluateSymbolSql(measure.path()[0], measure.path()[1], measure.measureDefinition());
  }

  autoPrefixWithCubeName(cubeName, sql, isMemberExpr = false) {
    if (!isMemberExpr && sql.match(/^[_a-zA-Z][_a-zA-Z0-9]*$/)) {
      return `${this.cubeAlias(cubeName)}.${sql}`;
    }
    return sql;
  }

  wrapSegmentForDimensionSelect(sql) {
    return sql;
  }

  pushCubeNameForCollectionIfNecessary(cubeName) {
    if ((this.evaluateSymbolContext || {}).cubeNames && cubeName) {
      this.evaluateSymbolContext.cubeNames.push(cubeName);
    }
  }

  pushJoinHints(joinHints) {
    if (this.safeEvaluateSymbolContext().joinHints && joinHints) {
      if (Array.isArray(joinHints) && joinHints.length === 1) {
        [joinHints] = joinHints;
      }
      this.safeEvaluateSymbolContext().joinHints.push(joinHints);
    }
  }

  pushMemberNameForCollectionIfNecessary(cubeName, name) {
    const pathFromArray = this.cubeEvaluator.pathFromArray([cubeName, name]);
    if (!this.cubeEvaluator.getCubeDefinition(cubeName).isView) {
      const joinHints = this.cubeEvaluator.joinHints();
      if (joinHints && joinHints.length) {
        joinHints.forEach(cube => this.pushCubeNameForCollectionIfNecessary(cube));
        this.pushJoinHints(joinHints);
      } else {
        this.pushCubeNameForCollectionIfNecessary(cubeName);
        this.pushJoinHints(cubeName);
      }
    }
    const context = this.safeEvaluateSymbolContext();
    if (context.memberNames && name) {
      context.memberNames.push(pathFromArray);
    }
  }

  safeEvaluateSymbolContext() {
    return this.evaluateSymbolContext || {};
  }

  evaluateSymbolSql(cubeName, name, symbol, memberExpressionType, subPropertyName) {
    const isMemberExpr = !!memberExpressionType;
    if (!memberExpressionType) {
      this.pushMemberNameForCollectionIfNecessary(cubeName, name);
    }
    if (symbol.patchedFrom) {
      this.pushMemberNameForCollectionIfNecessary(symbol.patchedFrom.cubeName, symbol.patchedFrom.name);
    }
    const memberPathArray = [cubeName, name];
    // Member path needs to be expanded to granularity if subPropertyName is provided.
    // Without this: infinite recursion with maximum call stack size exceeded.
    // During resolving within dimensionSql() the same symbol is pushed into the stack.
    // This would not be needed when the subProperty evaluation will be here and no
    // call to dimensionSql().
    if (subPropertyName && symbol.type === 'time') {
      memberPathArray.push('granularities', subPropertyName);
    }
    const memberPath = this.cubeEvaluator.pathFromArray(memberPathArray);
    let type = memberExpressionType;
    if (!type) {
      if (this.cubeEvaluator.isMeasure(memberPathArray)) {
        type = 'measure';
      } else if (this.cubeEvaluator.isDimension(memberPathArray)) {
        type = 'dimension';
      } else if (this.cubeEvaluator.isSegment(memberPathArray)) {
        type = 'segment';
      }
    }

    const parentMember = this.safeEvaluateSymbolContext().currentMember;
    if (this.safeEvaluateSymbolContext().memberChildren && parentMember) {
      this.safeEvaluateSymbolContext().memberChildren[parentMember] = this.safeEvaluateSymbolContext().memberChildren[parentMember] || [];
      if (this.safeEvaluateSymbolContext().memberChildren[parentMember].indexOf(memberPath) === -1) {
        this.safeEvaluateSymbolContext().memberChildren[parentMember].push(memberPath);
      }
    }

    this.safeEvaluateSymbolContext().currentMember = memberPath;
    try {
      if (type === 'measure') {
        let parentMeasure;
        if (this.safeEvaluateSymbolContext().compositeCubeMeasures ||
          this.safeEvaluateSymbolContext().leafMeasures) {
          parentMeasure = this.safeEvaluateSymbolContext().currentMeasure;
          if (this.safeEvaluateSymbolContext().compositeCubeMeasures) {
            if (parentMeasure && !memberExpressionType &&
              (
                this.cubeEvaluator.cubeNameFromPath(parentMeasure) !== cubeName ||
                this.newMeasure(memberPath).isCumulative()
              )
            ) {
              this.safeEvaluateSymbolContext().compositeCubeMeasures[parentMeasure] = true;
            }
          }
          this.safeEvaluateSymbolContext().currentMeasure = memberPath;
          if (this.safeEvaluateSymbolContext().leafMeasures) {
            if (parentMeasure) {
              this.safeEvaluateSymbolContext().leafMeasures[parentMeasure] = false;
            }
            this.safeEvaluateSymbolContext().leafMeasures[this.safeEvaluateSymbolContext().currentMeasure] = true;
          }
        }
        const primaryKeys = this.cubeEvaluator.primaryKeys[cubeName];
        const orderBySql = (symbol.orderBy || []).map(o => ({ sql: this.evaluateSql(cubeName, o.sql), dir: o.dir }));
        let sql;
        if (symbol.type !== 'rank') {
          sql = symbol.sql && this.evaluateSql(cubeName, symbol.sql) ||
            primaryKeys.length && (
              primaryKeys.length > 1 ?
                this.concatStringsSql(primaryKeys.map((pk) => this.castToString(this.primaryKeySql(pk, cubeName))))
                : this.primaryKeySql(primaryKeys[0], cubeName)
            ) || '*';
        }
        const result = this.renderSqlMeasure(
          name,
          sql && this.applyMeasureFilters(
            this.autoPrefixWithCubeName(
              cubeName,
              sql,
              isMemberExpr,
            ),
            symbol,
            cubeName
          ),
          symbol,
          cubeName,
          parentMeasure,
          orderBySql,
        );
        if (
          this.safeEvaluateSymbolContext().compositeCubeMeasures ||
          this.safeEvaluateSymbolContext().leafMeasures
        ) {
          this.safeEvaluateSymbolContext().currentMeasure = parentMeasure;
        }

        return result;
      } else if (type === 'dimension') {
        if ((this.safeEvaluateSymbolContext().renderedReference || {})[memberPath]) {
          return this.evaluateSymbolContext.renderedReference[memberPath];
        }
        // if (symbol.multiStage) {
        //   const orderBySql = (symbol.orderBy || []).map(o => ({ sql: this.evaluateSql(cubeName, o.sql), dir: o.dir }));
        //   const partitionBy = this.multiStageDimensions.length ? `PARTITION BY ${this.multiStageDimensions.map(d => d.dimensionSql()).join(', ')} ` : '';
        //   if (symbol.type === 'rank') {
        //     return `${symbol.type}() OVER (${partitionBy}ORDER BY ${orderBySql.map(o => `${o.sql} ${o.dir}`).join(', ')})`;
        //   }
        // }
        if (symbol.subQuery) {
          if (this.safeEvaluateSymbolContext().subQueryDimensions) {
            this.safeEvaluateSymbolContext().subQueryDimensions.push(memberPath);
          }
          return this.escapeColumnName(this.aliasName(memberPath));
        }
        if (symbol.case) {
          return this.renderDimensionCase(symbol, cubeName);
        } else if (symbol.type === 'geo') {
          return this.concatStringsSql([
            this.autoPrefixAndEvaluateSql(cubeName, symbol.latitude.sql, isMemberExpr),
            '\',\'',
            this.autoPrefixAndEvaluateSql(cubeName, symbol.longitude.sql, isMemberExpr)
          ]);
        } else if (symbol.type === 'time' && subPropertyName) {
          // TODO: Beware! memberExpression && shiftInterval are not supported with the current implementation.
          // Ideally this should be implemented (at least partially) here + inside cube symbol evaluation logic.
          // As now `dimensionSql()` is recursively calling `evaluateSymbolSql()` which is not good.
          const td = this.newTimeDimension({
            dimension: this.cubeEvaluator.pathFromArray([cubeName, name]),
            granularity: subPropertyName
          });
          // for time dimension with granularity convertedToTz() is called internally in dimensionSql() flow,
          // so we need to ignore convertTz later even if context convertTzForRawTimeDimension is set to true
          return this.evaluateSymbolSqlWithContext(
            () => td.dimensionSql(),
            { ignoreConvertTzForTimeDimension: true },
          );
        } else {
          let res = this.autoPrefixAndEvaluateSql(cubeName, symbol.sql, isMemberExpr);
          const memPath = this.cubeEvaluator.pathFromArray([cubeName, name]);

          // Skip view's member evaluation as there will be underlying cube's same member evaluation
          if (symbol.type === 'time' && !this.cubeEvaluator.cubeFromPath(memPath).isView) {
            if (this.safeEvaluateSymbolContext().timeShifts?.[memPath]) {
              if (symbol.shiftInterval) {
                throw new UserError(`Hierarchical time shift is not supported but was provided for '${memPath}'. Parent time shift is '${symbol.shiftInterval}' and current is '${this.safeEvaluateSymbolContext().timeShifts?.[memPath]}'`);
              }
              res = `(${this.addTimestampInterval(res, this.safeEvaluateSymbolContext().timeShifts?.[memPath])})`;
            } else if (this.safeEvaluateSymbolContext().commonTimeShift) {
              if (symbol.shiftInterval) {
                throw new UserError(`Hierarchical time shift is not supported but was provided for '${memPath}'. Parent time shift is '${symbol.shiftInterval}' and current is '${this.safeEvaluateSymbolContext().commonTimeShift}'`);
              }
              res = `(${this.addTimestampInterval(res, this.safeEvaluateSymbolContext().commonTimeShift)})`;
            }
          }

          if (this.safeEvaluateSymbolContext().convertTzForRawTimeDimension &&
            !this.safeEvaluateSymbolContext().ignoreConvertTzForTimeDimension &&
            !memberExpressionType &&
            symbol.type === 'time' &&
            this.cubeEvaluator.byPathAnyType(memberPathArray).ownedByCube
          ) {
            res = this.convertTz(res);
          }
          return res;
        }
      } else if (type === 'segment') {
        if ((this.safeEvaluateSymbolContext().renderedReference || {})[memberPath]) {
          return this.evaluateSymbolContext.renderedReference[memberPath];
        }
        return this.autoPrefixWithCubeName(cubeName, this.evaluateSql(cubeName, symbol.sql), isMemberExpr);
      }
      return this.evaluateSql(cubeName, symbol.sql);
    } finally {
      this.safeEvaluateSymbolContext().currentMember = parentMember;
    }
  }

  autoPrefixAndEvaluateSql(cubeName, sql, isMemberExpr = false) {
    return this.autoPrefixWithCubeName(cubeName, this.evaluateSql(cubeName, sql), isMemberExpr);
  }

  concatStringsSql(strings) {
    return strings.join(' || ');
  }

  /**
   *
   * @param {string} cubeName
   * @returns {Array<string>}
   */
  primaryKeyNames(cubeName) {
    const primaryKeys = this.cubeEvaluator.primaryKeys[cubeName];
    if (!primaryKeys || !primaryKeys.length) {
      throw new UserError(`One or more Primary key is required for '${cubeName}' cube`);
    }
    return primaryKeys.map((pk) => this.primaryKeyName(cubeName, pk));
  }

  primaryKeyName(cubeName, primaryKey) {
    return `${cubeName}.${primaryKey}`;
  }

  evaluateSql(cubeName, sql, options) {
    options = options || {};
    const self = this;
    const { cubeEvaluator } = this;
    return cubeEvaluator.resolveSymbolsCall(sql, (name) => {
      const nextCubeName = cubeEvaluator.symbols[name] && name || cubeName;
      const resolvedSymbol =
        cubeEvaluator.resolveSymbol(
          cubeName,
          name
        );
      // eslint-disable-next-line no-underscore-dangle
      if (resolvedSymbol._objectWithResolvedProperties) {
        return resolvedSymbol;
      }
      return self.evaluateSymbolSql(nextCubeName, name, resolvedSymbol);
    }, {
      sqlResolveFn: options.sqlResolveFn || ((symbol, cube, propName, subPropName) => self.evaluateSymbolSql(cube, propName, symbol, false, subPropName)),
      cubeAliasFn: self.cubeAlias.bind(self),
      contextSymbols: this.parametrizedContextSymbols(),
      query: this,
      collectJoinHints: true,
    });
  }

  withCubeAliasPrefix(cubeAliasPrefix, fn) {
    return this.evaluateSymbolSqlWithContext(fn, { cubeAliasPrefix });
  }

  /**
   * Evaluate escaped SQL-alias for cube or cube's property
   * (measure, dimension).
   * @param {string} cubeName
   * @returns string
   */
  cubeAlias(cubeName) {
    const prefix = this.safeEvaluateSymbolContext().cubeAliasPrefix || this.cubeAliasPrefix;
    return this.escapeColumnName(
      this.aliasName(
        `${prefix
          ? prefix + '__' + this.aliasName(cubeName)
          : cubeName}`
      )
    );
  }

  /**
   *
   * @param fn
   * @returns {Array<string>}
   */
  collectCubeNamesFor(fn) {
    const context = { cubeNames: [] };
    this.evaluateSymbolSqlWithContext(
      fn,
      context
    );

    return R.uniq(context.cubeNames);
  }

  collectJoinHintsFor(fn) {
    const context = { joinHints: [] };
    this.evaluateSymbolSqlWithContext(
      fn,
      context
    );
    return context.joinHints;
  }

  /**
   *
   * @param fn
   * @returns {Array<string>}
   */
  collectMemberNamesFor(fn) {
    const context = { memberNames: [] };
    this.evaluateSymbolSqlWithContext(
      fn,
      context
    );

    return R.uniq(context.memberNames);
  }

  collectAllMemberNames() {
    return R.flatten(this.collectFromMembers(false, this.collectMemberNamesFor.bind(this), 'collectAllMemberNames'));
  }

  collectMultipliedMeasures(context) {
    return (fn) => {
      const foundCompositeCubeMeasures = {};
      this.evaluateSymbolSqlWithContext(
        fn,
        { ...context, compositeCubeMeasures: foundCompositeCubeMeasures }
      );

      const renderContext = {
        ...context, measuresToRender: [], foundCompositeCubeMeasures, compositeCubeMeasures: {}, rootMeasure: {}
      };
      this.evaluateSymbolSqlWithContext(
        fn,
        renderContext
      );
      return renderContext.measuresToRender.length ?
        R.uniq(renderContext.measuresToRender) :
        [renderContext.rootMeasure.value];
    };
  }

  collectLeafMeasures(fn) {
    const context = { leafMeasures: {} };
    this.evaluateSymbolSqlWithContext(
      fn,
      context
    );
    return R.pipe(
      R.toPairs,
      R.map(([measure, isLeaf]) => isLeaf && measure),
      R.filter(R.identity)
    )(context.leafMeasures);
  }

  /**
   * @template T
   * @param {() => T} fn
   * @param {unknown} context
   * @returns {T}
   */
  evaluateSymbolSqlWithContext(fn, context) {
    const oldContext = this.evaluateSymbolContext;
    this.evaluateSymbolContext = oldContext ? Object.assign({}, oldContext, context) : context;
    try {
      const result = fn();
      this.evaluateSymbolContext = oldContext;
      return result;
    } finally {
      this.evaluateSymbolContext = oldContext;
    }
  }

  renderSqlMeasure(name, evaluateSql, symbol, cubeName, parentMeasure, orderBySql) {
    const multiplied = this.multipliedJoinRowResult(cubeName) || false;
    const measurePath = `${cubeName}.${name}`;
    let resultMultiplied = multiplied;
    if (multiplied && (
      symbol.type === 'countDistinct' ||
      !this.safeEvaluateSymbolContext().hasMultipliedForPreAggregation && (
        symbol.type === 'number' && evaluateSql === 'count(*)' ||
        symbol.type === 'count' && !symbol.sql
      )
    )) {
      resultMultiplied = false;
    }
    if (parentMeasure &&
      (this.safeEvaluateSymbolContext().foundCompositeCubeMeasures || {})[parentMeasure] &&
      !(this.safeEvaluateSymbolContext().foundCompositeCubeMeasures || {})[measurePath]
    ) {
      this.safeEvaluateSymbolContext().measuresToRender.push({ multiplied: resultMultiplied, measure: measurePath, multiStage: symbol.multiStage });
    }
    if (this.safeEvaluateSymbolContext().foundCompositeCubeMeasures && !parentMeasure) {
      this.safeEvaluateSymbolContext().rootMeasure.value = { multiplied: resultMultiplied, measure: measurePath, multiStage: symbol.multiStage };
    }
    if (((this.evaluateSymbolContext || {}).renderedReference || {})[measurePath]) {
      return this.evaluateSymbolContext.renderedReference[measurePath];
    }
    if (
      this.safeEvaluateSymbolContext().ungrouped ||
      this.safeEvaluateSymbolContext().ungroupedForWrappingGroupBy
    ) {
      return evaluateSql === '*' ? '1' : evaluateSql;
    }
    if (this.ungrouped) {
      if (this.safeEvaluateSymbolContext().ungroupedAliases?.[measurePath]) {
        evaluateSql = this.safeEvaluateSymbolContext().ungroupedAliases[measurePath];
      }
      if ((this.safeEvaluateSymbolContext().ungroupedAliasesForCumulative || {})[measurePath]) {
        evaluateSql = this.safeEvaluateSymbolContext().ungroupedAliasesForCumulative[measurePath];
      }

      if (symbol.type === 'count' || symbol.type === 'countDistinct' || symbol.type === 'countDistinctApprox') {
        const sql = this.caseWhenStatement([{ sql: `(${evaluateSql}) IS NOT NULL`, label: '1' }]);
        return evaluateSql === '*' ? '1' : sql;
      } else {
        return evaluateSql;
      }
    }
    if ((this.safeEvaluateSymbolContext().ungroupedAliases || {})[measurePath]) {
      evaluateSql = (this.safeEvaluateSymbolContext().ungroupedAliases || {})[measurePath];
    }
    if ((this.safeEvaluateSymbolContext().ungroupedAliasesForCumulative || {})[measurePath]) {
      evaluateSql = (this.safeEvaluateSymbolContext().ungroupedAliasesForCumulative || {})[measurePath];
      const { topLevelMerge } = this.safeEvaluateSymbolContext();
      const onGroupedColumn = this.aggregateOnGroupedColumn(
        symbol, evaluateSql, topLevelMerge != null ? topLevelMerge : true, measurePath
      );
      if (onGroupedColumn) {
        return onGroupedColumn;
      }
    }
    if (symbol.multiStage) {
      const partitionBy = (this.multiStageDimensions.length || this.multiStageTimeDimensions.length) ?
        `PARTITION BY ${this.multiStageDimensions.concat(this.multiStageTimeDimensions).map(d => d.dimensionSql()).join(', ')} ` : '';
      if (symbol.type === 'rank') {
        return `${symbol.type}() OVER (${partitionBy}ORDER BY ${orderBySql.map(o => `${o.sql} ${o.dir}`).join(', ')})`;
      }
      if (!(
        R.equals(this.multiStageDimensions.map(d => d.expressionPath()), this.dimensions.map(d => d.expressionPath())) &&
        R.equals(this.multiStageTimeDimensions.map(d => d.expressionPath()), this.timeDimensions.map(d => d.expressionPath()))
      )) {
        let funDef;
        if (symbol.type === 'countDistinctApprox') {
          funDef = this.countDistinctApprox(evaluateSql);
        } else if (symbol.type === 'countDistinct' || symbol.type === 'count' && !symbol.sql && multiplied) {
          funDef = `count(distinct ${evaluateSql})`;
        } else if (CubeSymbols.isCalculatedMeasureType(symbol.type)) {
          // TODO calculated measure type will be ungrouped
          // if (this.multiStageDimensions.length !== this.dimensions.length) {
          //   throw new UserError(`Calculated measure '${measurePath}' uses group_by or reduce_by context modifiers while it isn't allowed`);
          // }
          return evaluateSql;
        } else {
          funDef = `${symbol.type}(${symbol.type}(${evaluateSql}))`;
        }
        return `${funDef} OVER(${partitionBy})`;
      }
    }
    if (symbol.type === 'countDistinctApprox') {
      return this.safeEvaluateSymbolContext().overTimeSeriesAggregate || this.options.preAggregationQuery ?
        this.hllInit(evaluateSql) :
        this.countDistinctApprox(evaluateSql);
    } else if (symbol.type === 'countDistinct' || symbol.type === 'count' && !symbol.sql && multiplied) {
      return `count(distinct ${evaluateSql})`;
    } else if (symbol.type === 'runningTotal') {
      return `sum(${evaluateSql})`; // TODO
    }
    if (multiplied) {
      if (symbol.type === 'number' && evaluateSql === 'count(*)') {
        return this.primaryKeyCount(cubeName, true);
      }
    }
    if (CubeSymbols.isCalculatedMeasureType(symbol.type)) {
      return evaluateSql;
    }
    return `${symbol.type}(${evaluateSql})`;
  }

  aggregateOnGroupedColumn(symbol, evaluateSql, topLevelMerge, measurePath) {
    const cumulativeMeasureFilters = (this.safeEvaluateSymbolContext().cumulativeMeasureFilters || {})[measurePath];
    if (cumulativeMeasureFilters) {
      const sql = cumulativeMeasureFilters.filterToWhere();
      if (sql) {
        evaluateSql = this.caseWhenStatement([{ sql, label: evaluateSql }]);
      }
    }
    if (symbol.type === 'count' || symbol.type === 'sum') {
      return `sum(${evaluateSql})`;
    } else if (symbol.type === 'countDistinctApprox') {
      return topLevelMerge ? this.hllCardinalityMerge(evaluateSql) : this.hllMergeOnly(evaluateSql);
    } else if (symbol.type === 'min' || symbol.type === 'max') {
      return `${symbol.type}(${evaluateSql})`;
    }
    return undefined;
  }

  topAggregateWrap(symbol, evaluateSql) {
    if (symbol.type === 'countDistinctApprox') {
      return this.hllCardinality(evaluateSql);
    }
    return evaluateSql;
  }

  hllInit(_sql) {
    throw new UserError('Distributed approximate distinct count is not supported by this DB');
  }

  hllMerge(_sql) {
    throw new UserError('Distributed approximate distinct count is not supported by this DB');
  }

  hllCardinality(_sql) {
    throw new UserError('Distributed approximate distinct count is not supported by this DB');
  }

  hllMergeOnly(sql) {
    return this.hllMerge(sql);
  }

  hllCardinalityMerge(sql) {
    return this.hllMerge(sql);
  }

  castToString(sql) {
    return `CAST(${sql} as TEXT)`;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  countDistinctApprox(sql) {
    throw new UserError('Approximate distinct count is not supported by this DB');
  }

  primaryKeyCount(cubeName, distinct) {
    const primaryKeys = this.cubeEvaluator.primaryKeys[cubeName];
    const primaryKeySql = primaryKeys.length > 1 ?
      this.concatStringsSql(primaryKeys.map((pk) => this.castToString(this.primaryKeySql(pk, cubeName)))) :
      this.primaryKeySql(primaryKeys[0], cubeName);
    return `count(${distinct ? 'distinct ' : ''}${primaryKeySql})`;
  }

  renderDimensionCase(symbol, cubeName) {
    const when = symbol.case.when.map(w => ({
      sql: this.evaluateSql(cubeName, w.sql),
      label: this.renderDimensionCaseLabel(w.label, cubeName)
    }));
    return this.caseWhenStatement(
      when,
      symbol.case.else && this.renderDimensionCaseLabel(symbol.case.else.label, cubeName)
    );
  }

  renderDimensionCaseLabel(label, cubeName) {
    if (typeof label === 'object' && label.sql) {
      return this.evaluateSql(cubeName, label.sql);
    }
    return `'${label}'`;
  }

  caseWhenStatement(when, elseLabel) {
    return `CASE
    ${when.map(w => `WHEN ${w.sql} THEN ${w.label}`).join('\n')}${elseLabel ? ` ELSE ${elseLabel}` : ''} END`;
  }

  applyMeasureFilters(evaluateSql, symbol, cubeName) {
    if (!symbol.filters || !symbol.filters.length) {
      return evaluateSql;
    }

    const where = this.evaluateMeasureFilters(symbol, cubeName);

    return `CASE WHEN ${where} THEN ${evaluateSql === '*' ? '1' : evaluateSql} END`;
  }

  evaluateMeasureFilters(symbol, cubeName) {
    return this.evaluateFiltersArray(symbol.filters, cubeName);
  }

  evaluateFiltersArray(filtersArray, cubeName) {
    return filtersArray.map(f => this.evaluateSql(cubeName, f.sql))
      .map(s => `(${s})`).join(' AND ');
  }

  /**
   * @param {string} primaryKeyName
   * @param {string} cubeName
   * @returns {unknown}
   */
  primaryKeySql(primaryKeyName, cubeName) {
    const primaryKeyDimension = this.cubeEvaluator.dimensionByPath([cubeName, primaryKeyName]);
    return this.evaluateSymbolSql(
      cubeName,
      primaryKeyName,
      primaryKeyDimension
    );
  }

  /**
   * @param cubeName
   * @returns Boolean
   */
  multipliedJoinRowResult(cubeName) {
    // this.join not initialized on collectCubeNamesForSql
    return this.join && this.join.multiplicationFactor[cubeName];
  }

  inDbTimeZone(date) {
    return localTimestampToUtc(this.timezone, this.timestampFormat(), date);
  }

  /**
   * @return {string}
   */
  timestampFormat() {
    return 'YYYY-MM-DD[T]HH:mm:ss.SSS[Z]';
  }

  /**
   * @return {number}
   */
  timestampPrecision() {
    return 3;
  }

  /**
   * @param {string} field
   * @return {string}
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  convertTz(field) {
    throw new Error('Not implemented');
  }

  /**
   * @param {string} granularity
   * @param {string} dimension
   * @return {string}
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  timeGroupedColumn(granularity, dimension) {
    throw new Error('Not implemented');
  }

  /**
   * Returns sql for source expression floored to timestamps aligned with
   * intervals relative to origin timestamp point
   * @param {string} interval (a value expression of type interval)
   * @param {string} source (a value expression of type timestamp/date)
   * @param {string} origin (a value expression of type timestamp/date without timezone)
   * @returns {string}
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  dateBin(interval, source, origin) {
    throw new Error('Date bin function, required for custom time dimension granularities, is not implemented for this data source');
    // Different syntax possible in different DBs
  }

  /**
   * Returns the lowest time unit for the interval
   * @protected
   * @param {string} interval
   * @returns {string}
   */
  diffTimeUnitForInterval(interval) {
    if (/second/i.test(interval)) {
      return 'second';
    } else if (/minute/i.test(interval)) {
      return 'minute';
    } else if (/hour/i.test(interval)) {
      return 'hour';
    } else if (/day/i.test(interval)) {
      return 'day';
    } else if (/week/i.test(interval)) {
      return 'day';
    } else if (/month/i.test(interval)) {
      return 'month';
    } else if (/quarter/i.test(interval)) {
      return 'month';
    } else /* if (/year/i.test(interval)) */ {
      return 'year';
    }
  }

  /**
   * @param {string} dimension
   * @param {import('./Granularity').Granularity} granularity
   * @return {string}
   */
  dimensionTimeGroupedColumn(dimension, granularity) {
    let dtDate;

    // Interval is aligned with natural calendar, so we can use DATE_TRUNC
    if (granularity.isNaturalAligned()) {
      if (granularity.granularityOffset) {
        // Example: DATE_TRUNC(interval, dimension - INTERVAL 'offset') + INTERVAL 'offset'
        dtDate = this.subtractInterval(dimension, granularity.granularityOffset);
        dtDate = this.timeGroupedColumn(granularity.granularityFromInterval(), dtDate);
        dtDate = this.addInterval(dtDate, granularity.granularityOffset);

        return dtDate;
      }

      return this.timeGroupedColumn(granularity.granularityFromInterval(), dimension);
    }

    return this.dateBin(granularity.granularityInterval, dimension, granularity.originLocalFormatted());
  }

  /**
   * Evaluate alias for specific cube's property.
   * @param {string} name Property name.
   * @param {boolean?} isPreAggregationName Pre-agg flag.
   * @returns {string}
   */
  aliasName(name, isPreAggregationName = false) {
    if (this.options.memberToAlias && this.options.memberToAlias[name]) {
      return this.options.memberToAlias[name];
    }
    const path = name.split('.');
    if (path[0] && this.cubeEvaluator.cubeExists(path[0]) && this.cubeEvaluator.cubeFromPath(path[0]).sqlAlias) {
      const cubeName = path[0];
      path.splice(0, 1);
      path.unshift(this.cubeEvaluator.cubeFromPath(cubeName).sqlAlias);
      name = this.cubeEvaluator.pathFromArray(path);
    }
    // TODO: https://github.com/cube-js/cube.js/issues/4019
    // use single underscore for pre-aggregations to avoid fail of pre-aggregation name replace
    const lowercaseName = name.toLowerCase();
    if (lowercaseName === '__user' || lowercaseName === '__cubejoinfield') {
      return name;
    }
    return inflection.underscore(name).replace(/\./g, isPreAggregationName ? '_' : '__');
  }

  /**
   *
   * @param {unknown} options
   * @returns {this}
   */
  newSubQuery(options) {
    const QueryClass = this.constructor;
    return new QueryClass(this.compilers, this.subQueryOptions(options));
  }

  newSubQueryForCube(cube, options) {
    options = { ...options };
    if (this.options.queryFactory) {
      // When dealing with rollup joins, it's crucial to use the correct parameter allocator for the specific cube in use.
      // By default, we'll use BaseQuery, but it's important to note that different databases (Oracle, PostgreSQL, MySQL, Druid, etc.)
      // have unique parameter allocator symbols. Using the wrong allocator can break the query, especially when rollup joins involve
      // different cubes that require different allocators.
      return this.options.queryFactory.createQuery(cube, this.compilers, { ...this.subQueryOptions(options), paramAllocator: null });
    }

    return this.newSubQuery(options);
  }

  subQueryOptions(options) {
    return {
      paramAllocator: this.paramAllocator,
      timezone: this.timezone,
      preAggregationQuery: this.options.preAggregationQuery,
      useOriginalSqlPreAggregationsInPreAggregation: this.options.useOriginalSqlPreAggregationsInPreAggregation,
      contextSymbols: this.contextSymbols,
      preAggregationsSchema: this.preAggregationsSchemaOption,
      cubeLatticeCache: this.options.cubeLatticeCache,
      historyQueries: this.options.historyQueries,
      externalQueryClass: this.options.externalQueryClass,
      queryFactory: this.options.queryFactory,
      useNativeSqlPlanner: this.options.useNativeSqlPlanner,
      ...options,
    };
  }

  cacheKeyQueries(transformFn) { // TODO collect sub queries
    if (!this.safeEvaluateSymbolContext().preAggregationQuery) {
      const preAggregationForQuery = this.preAggregations.findPreAggregationForQuery();
      if (preAggregationForQuery) {
        return [];
      }
    }

    return this.refreshKeysByCubes(this.allCubeNames, transformFn);
  }

  refreshKeysByCubes(cubes, transformFn) {
    const refreshKeyQueryByCube = (cube) => {
      const cubeFromPath = this.cubeEvaluator.cubeFromPath(cube);
      if (cubeFromPath.refreshKey) {
        if (cubeFromPath.refreshKey.sql) {
          return [
            this.evaluateSql(cube, cubeFromPath.refreshKey.sql),
            {
              external: false,
              renewalThreshold: cubeFromPath.refreshKey.every
                ? this.refreshKeyRenewalThresholdForInterval(cubeFromPath.refreshKey, false)
                : this.defaultRefreshKeyRenewalThreshold()
            },
            this
          ];
        }

        if (cubeFromPath.refreshKey.every) {
          const [sql, external, query] = this.everyRefreshKeySql(cubeFromPath.refreshKey);
          return [
            this.refreshKeySelect(sql),
            {
              external,
              renewalThreshold: this.refreshKeyRenewalThresholdForInterval(cubeFromPath.refreshKey)
            },
            query
          ];
        }
      }

      const [sql, external, query] = this.everyRefreshKeySql(this.defaultEveryRefreshKey());
      return [
        this.refreshKeySelect(sql),
        {
          external,
          renewalThreshold: this.defaultRefreshKeyRenewalThreshold()
        },
        query
      ];
    };

    return cubes.map(cube => [cube, refreshKeyQueryByCube(cube)])
      .map(([cube, refreshKeyTuple]) => (transformFn ? transformFn(cube, refreshKeyTuple) : refreshKeyTuple))
      .map(([sql, options, query]) => query.paramAllocator.buildSqlAndParams(sql).concat(options));
  }

  aggSelectForDimension(cube, dimension, aggFunction) {
    const cubeNamesForTimeDimension = this.collectFrom(
      [dimension],
      this.collectCubeNamesFor.bind(this),
      'collectCubeNamesFor'
    );
    if (cubeNamesForTimeDimension.length === 1 && cubeNamesForTimeDimension[0] === cube) {
      const dimensionSql = this.dimensionSql(dimension);
      return `select ${aggFunction}(${this.convertTz(dimensionSql)}) from ${this.cubeSql(cube)} ${this.asSyntaxTable} ${this.cubeAlias(cube)}`;
    }
    return null;
  }

  cubeCardinalityQueries() { // TODO collect sub queries
    return R.fromPairs(this.allCubeNames
      .map(cube => [
        cube,
        this.paramAllocator.buildSqlAndParams(`select count(*) as ${this.escapeColumnName('total_count')} from ${this.cubeSql(cube)} ${this.asSyntaxTable} ${this.cubeAlias(cube)}`)
      ]));
  }

  renewalThreshold(refreshKeyAllSetManually) {
    return refreshKeyAllSetManually ? 24 * 60 * 60 : 6 * 60 * 60;
  }

  nowTimestampSql() {
    return 'NOW()';
  }

  unixTimestampSql() {
    return `EXTRACT(EPOCH FROM ${this.nowTimestampSql()})`;
  }

  preAggregationTableName(cube, preAggregationName, skipSchema) {
    const tblName = this.aliasName(`${cube}.${preAggregationName}`, true);
    return `${skipSchema ? '' : this.preAggregationSchema() && `${this.preAggregationSchema()}.`}${tblName}`;
  }

  preAggregationSchema() {
    return this.preAggregationsSchemaOption;
  }

  preAggregationLoadSql(cube, preAggregation, tableName) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    return [`CREATE TABLE ${tableName} ${this.asSyntaxTable} ${sqlAndParams[0]}`, sqlAndParams[1]];
  }

  preAggregationPreviewSql(tableName) {
    return this.paramAllocator.buildSqlAndParams(`SELECT * FROM ${tableName} LIMIT 1000`);
  }

  indexSql(cube, preAggregation, index, indexName, tableName) {
    if (preAggregation.external && this.externalQueryClass) {
      return this.externalQuery().indexSql(cube, preAggregation, index, indexName, tableName);
    }

    if (index.columns) {
      const escapedColumns = this.evaluateIndexColumns(cube, index);
      return this.paramAllocator.buildSqlAndParams(this.createIndexSql(indexName, tableName, escapedColumns));
    } else {
      throw new Error('Index SQL support is not implemented');
    }
  }

  evaluateIndexColumns(cube, index) {
    const columns = this.cubeEvaluator.evaluateReferences(cube, index.columns, { originalSorting: true });
    return columns.map(column => {
      const path = column.split('.');
      if (path[0] &&
        this.cubeEvaluator.cubeExists(path[0]) &&
        (
          this.cubeEvaluator.isMeasure(path) ||
          this.cubeEvaluator.isDimension(path) ||
          this.cubeEvaluator.isSegment(path)
        )
      ) {
        return this.aliasName(column);
      } else {
        return column;
      }
    }).map(c => this.escapeColumnName(c));
  }

  createIndexSql(indexName, tableName, escapedColumns) {
    return `CREATE INDEX ${indexName} ON ${tableName} (${escapedColumns.join(', ')})`;
  }

  preAggregationSql(cube, preAggregation) {
    return this.cacheValue(
      ['preAggregationSql', cube, JSON.stringify(preAggregation)],
      () => {
        const { collectOriginalSqlPreAggregations } = this.safeEvaluateSymbolContext();
        if (preAggregation.type === 'autoRollup') {
          const query = this.preAggregations.autoRollupPreAggregationQuery(cube, preAggregation);
          return query.evaluateSymbolSqlWithContext(() => query.buildSqlAndParams(), {
            collectOriginalSqlPreAggregations
          });
        } else if (preAggregation.type === 'rollup') {
          const query = this.preAggregations.rollupPreAggregationQuery(cube, preAggregation);
          return query.evaluateSymbolSqlWithContext(() => query.buildSqlAndParams(), {
            collectOriginalSqlPreAggregations
          });
        } else if (preAggregation.type === 'originalSql') {
          const originalSqlPreAggregationQuery = this.preAggregations.originalSqlPreAggregationQuery(
            cube,
            preAggregation
          );
          return this.paramAllocator.buildSqlAndParams(originalSqlPreAggregationQuery.evaluateSymbolSqlWithContext(
            () => originalSqlPreAggregationQuery.evaluateSql(cube, this.cubeEvaluator.cubeFromPath(cube).sql),
            { preAggregationQuery: true, collectOriginalSqlPreAggregations }
          ));
        }
        throw new UserError(`Unknown pre-aggregation type '${preAggregation.type}' in '${cube}'`);
      },
      { inputProps: { collectOriginalSqlPreAggregations: [] }, cache: this.queryCache }
    );
  }

  preAggregationOutputColumnTypes(cube, preAggregation) {
    return this.cacheValue(
      ['preAggregationOutputColumnTypes', cube, JSON.stringify(preAggregation)],
      () => {
        if (!preAggregation.outputColumnTypes) {
          return null;
        }

        if (preAggregation.type === 'rollup') {
          const query = this.preAggregations.rollupPreAggregationQuery(cube, preAggregation);

          const evaluatedMapOutputColumnTypes = preAggregation.outputColumnTypes.reduce((acc, outputColumnType) => {
            acc.set(outputColumnType.name, outputColumnType);
            return acc;
          }, new Map());

          const findSchemaType = member => {
            const outputSchemaType = evaluatedMapOutputColumnTypes.get(member);
            if (!outputSchemaType) {
              throw new UserError(`Output schema type for ${member} not found in pre-aggregation ${preAggregation}`);
            }

            return {
              name: this.aliasName(member),
              type: outputSchemaType.type,
            };
          };

          // The order of the output columns is important, it should match the order in the select statement
          const outputColumnTypes = [
            ...(query.dimensions || []).map(d => findSchemaType(d.dimension)),
            ...(query.timeDimensions || []).map(t => ({
              name: `${this.aliasName(t.dimension)}_${t.granularity}`,
              type: 'TIMESTAMP'
            })),
            ...(query.measures || []).map(m => findSchemaType(m.measure)),
          ];

          return outputColumnTypes;
        }
        throw new UserError('Output schema is only supported for rollup pre-aggregations');
      },
      { inputProps: {}, cache: this.queryCache }
    );
  }

  preAggregationUniqueKeyColumns(cube, preAggregation) {
    if (preAggregation.uniqueKeyColumns) {
      return preAggregation.uniqueKeyColumns.map(key => this.aliasName(`${cube}.${key}`));
    }

    return this.dimensionColumns();
  }

  preAggregationReadOnly(_cube, _preAggregation) {
    return false;
  }

  preAggregationAllowUngroupingWithPrimaryKey(_cube, _preAggregation) {
    return false;
  }

  /**
   * @public
   * @returns {any}
   */
  sqlTemplates() {
    return {
      functions: {
        SUM: 'SUM({{ args_concat }})',
        MIN: 'MIN({{ args_concat }})',
        MAX: 'MAX({{ args_concat }})',
        COUNT: 'COUNT({{ args_concat }})',
        COUNT_DISTINCT: 'COUNT(DISTINCT {{ args_concat }})',
        AVG: 'AVG({{ args_concat }})',
        STDDEV_POP: 'STDDEV_POP({{ args_concat }})',
        STDDEV_SAMP: 'STDDEV_SAMP({{ args_concat }})',
        VAR_POP: 'VAR_POP({{ args_concat }})',
        VAR_SAMP: 'VAR_SAMP({{ args_concat }})',
        COVAR_POP: 'COVAR_POP({{ args_concat }})',
        COVAR_SAMP: 'COVAR_SAMP({{ args_concat }})',
        GROUP_ANY: 'max({{ expr }})',
        COALESCE: 'COALESCE({{ args_concat }})',
        CONCAT: 'CONCAT({{ args_concat }})',
        FLOOR: 'FLOOR({{ args_concat }})',
        CEIL: 'CEIL({{ args_concat }})',
        TRUNC: 'TRUNC({{ args_concat }})',

        // There is a difference in behaviour of these function processing in different DBs and DWHs.
        // The SQL standard requires greatest and least to return null in case one argument is null.
        // However, many DBMS ignore NULL values (mostly because greatest and least were often supported
        // decades before they were added to the SQL standard in 2023).
        // Cube follows the Postgres implementation (as we mimic the Postgres protocol) and ignores NULL values.
        // So these functions are enabled on a driver-specific basis for databases that ignores NULLs.
        // LEAST: 'LEAST({{ args_concat }})',
        // GREATEST: 'GREATEST({{ args_concat }})',

        LOWER: 'LOWER({{ args_concat }})',
        UPPER: 'UPPER({{ args_concat }})',
        LEFT: 'LEFT({{ args_concat }})',
        RIGHT: 'RIGHT({{ args_concat }})',
        SQRT: 'SQRT({{ args_concat }})',
        ABS: 'ABS({{ args_concat }})',
        ACOS: 'ACOS({{ args_concat }})',
        ASIN: 'ASIN({{ args_concat }})',
        ATAN: 'ATAN({{ args_concat }})',
        COS: 'COS({{ args_concat }})',
        EXP: 'EXP({{ args_concat }})',
        LN: 'LN({{ args_concat }})',
        LOG: 'LOG({{ args_concat }})',
        DLOG10: 'LOG10({{ args_concat }})',
        PI: 'PI()',
        POWER: 'POWER({{ args_concat }})',
        SIN: 'SIN({{ args_concat }})',
        TAN: 'TAN({{ args_concat }})',
        REPEAT: 'REPEAT({{ args_concat }})',
        NULLIF: 'NULLIF({{ args_concat }})',
        ROUND: 'ROUND({{ args_concat }})',

        STDDEV: 'STDDEV_SAMP({{ args_concat }})',
        SUBSTR: 'SUBSTRING({{ args_concat }})',
        CHARACTERLENGTH: 'CHAR_LENGTH({{ args[0] }})',

        // Non-ANSI functions
        BTRIM: 'BTRIM({{ args_concat }})',
        LTRIM: 'LTRIM({{ args_concat }})',
        RTRIM: 'RTRIM({{ args_concat }})',
        ATAN2: 'ATAN2({{ args_concat }})',
        COT: 'COT({{ args_concat }})',
        DEGREES: 'DEGREES({{ args_concat }})',
        RADIANS: 'RADIANS({{ args_concat }})',
        SIGN: 'SIGN({{ args_concat }})',
        ASCII: 'ASCII({{ args_concat }})',
        STRPOS: 'POSITION({{ args[1] }} IN {{ args[0] }})',
        REPLACE: 'REPLACE({{ args_concat }})',
        DATEDIFF: 'DATEDIFF({{ date_part }}, {{ args[1] }}, {{ args[2] }})',
        TO_CHAR: 'TO_CHAR({{ args_concat }})',
        // DATEADD is being rewritten to DATE_ADD
        // DATEADD: 'DATEADD({{ date_part }}, {{ interval }}, {{ args[2] }})',
        DATE: 'DATE({{ args_concat }})',

        PERCENTILECONT: 'PERCENTILE_CONT({{ args_concat }})',
      },
      statements: {
        select: '{% if ctes %} WITH \n' +
          '{{ ctes | join(\',\n\') }}\n' +
          '{% endif %}' +
          'SELECT {% if distinct %}DISTINCT {% endif %}' +
          '{{ select_concat | map(attribute=\'aliased\') | join(\', \') }} {% if from %}\n' +
          'FROM (\n' +
          '{{ from | indent(2, true) }}\n' +
          ') AS {{ from_alias }}{% elif from_prepared %}\n' +
          'FROM {{ from_prepared }}' +
          '{% endif %}' +
          '{% if filter %}\nWHERE {{ filter }}{% endif %}' +
          '{% if group_by %}\nGROUP BY {{ group_by }}{% endif %}' +
          '{% if having %}\nHAVING {{ having }}{% endif %}' +
          '{% if order_by %}\nORDER BY {{ order_by | map(attribute=\'expr\') | join(\', \') }}{% endif %}' +
          '{% if limit is not none %}\nLIMIT {{ limit }}{% endif %}' +
          '{% if offset is not none %}\nOFFSET {{ offset }}{% endif %}',
        group_by_exprs: '{{ group_by | map(attribute=\'index\') | join(\', \') }}',
        join: '{{ join_type }} JOIN {{ source }} ON {{ condition }}',
        cte: '{{ alias }} AS ({{ query | indent(2, true) }})',
        time_series_select: 'SELECT date_from::timestamp AS "date_from",\n' +
          'date_to::timestamp AS "date_to" \n' +
          'FROM(\n' +
          '    VALUES ' +
          '{% for time_item in seria  %}' +
          '(\'{{ time_item | join(\'\\\', \\\'\') }}\')' +
          '{% if not loop.last %}, {% endif %}' +
          '{% endfor %}' +
          ') AS dates (date_from, date_to)',
        time_series_get_range: 'SELECT {{ max_expr }} as {{ quoted_max_name }},\n' +
          '{{ min_expr }} as {{ quoted_min_name }}\n' +
          'FROM {{ from_prepared }}\n' +
          '{% if filter %}WHERE {{ filter }}{% endif %}'
      },
      expressions: {
        column_reference: '{% if table_name %}{{ table_name }}.{% endif %}{{ name }}',
        column_aliased: '{{expr}} {{quoted_alias}}',
        query_aliased: '{{ query }} AS {{ quoted_alias }}',
        case: 'CASE{% if expr %} {{ expr }}{% endif %}{% for when, then in when_then %} WHEN {{ when }} THEN {{ then }}{% endfor %}{% if else_expr %} ELSE {{ else_expr }}{% endif %} END',
        is_null: '({{ expr }} IS {% if negate %}NOT {% endif %}NULL)',
        binary: '({{ left }} {{ op }} {{ right }})',
        sort: '{{ expr }} {% if asc %}ASC{% else %}DESC{% endif %} NULLS {% if nulls_first %}FIRST{% else %}LAST{% endif %}',
        order_by: '{% if index %} {{ index }} {% else %} {{ expr }} {% endif %} {% if asc %}ASC{% else %}DESC{% endif %}{% if nulls_first %} NULLS FIRST{% endif %}',
        cast: 'CAST({{ expr }} AS {{ data_type }})',
        window_function: '{{ fun_call }} OVER ({% if partition_by_concat %}PARTITION BY {{ partition_by_concat }}{% if order_by_concat or window_frame %} {% endif %}{% endif %}{% if order_by_concat %}ORDER BY {{ order_by_concat }}{% if window_frame %} {% endif %}{% endif %}{% if window_frame %}{{ window_frame }}{% endif %})',
        window_frame_bounds: '{{ frame_type }} BETWEEN {{ frame_start }} AND {{ frame_end }}',
        in_list: '{{ expr }} {% if negated %}NOT {% endif %}IN ({{ in_exprs_concat }})',
        subquery: '({{ expr }})',
        in_subquery: '{{ expr }} {% if negated %}NOT {% endif %}IN {{ subquery_expr }}',
        rollup: 'ROLLUP({{ exprs_concat }})',
        cube: 'CUBE({{ exprs_concat }})',
        negative: '-({{ expr }})',
        not: 'NOT ({{ expr }})',
        add_interval: '{{ date }} + interval \'{{ interval }}\'',
        sub_interval: '{{ date }} - interval \'{{ interval }}\'',
        true: 'TRUE',
        false: 'FALSE',
        like: '{{ expr }} {% if negated %}NOT {% endif %}LIKE {{ pattern }}',
        ilike: '{{ expr }} {% if negated %}NOT {% endif %}ILIKE {{ pattern }}',
        like_escape: '{{ like_expr }} ESCAPE {{ escape_char }}',
        within_group: '{{ fun_sql }} WITHIN GROUP (ORDER BY {{ within_group_concat }})',
        concat_strings: '{{ strings | join(\' || \' ) }}',
        rolling_window_expr_timestamp_cast: '{{ value }}',
        timestamp_literal: '{{ value }}',
        between: '{{ expr }} {% if negated %}NOT {% endif %}BETWEEN {{ low }} AND {{ high }}',
      },
      tesseract: {
        ilike: '{{ expr }} {% if negated %}NOT {% endif %}ILIKE {{ pattern }}', // May require different overloads in Tesseract than the ilike from expressions used in SQLAPI.
        series_bounds_cast: '{{ expr }}',
        bool_param_cast: '{{ expr }}',
        number_param_cast: '{{ expr }}',
      },
      filters: {
        equals: '{{ column }} = {{ value }}{{ is_null_check }}',
        not_equals: '{{ column }} <> {{ value }}{{ is_null_check }}',
        or_is_null_check: ' OR {{ column }} IS NULL',
        set_where: '{{ column }} IS NOT NULL',
        not_set_where: '{{ column }} IS NULL',
        in: '{{ column }} IN ({{ values_concat }}){{ is_null_check }}',
        not_in: '{{ column }} NOT IN ({{ values_concat }}){{ is_null_check }}',
        time_range_filter: '{{ column }} >= {{ from_timestamp }} AND {{ column }} <= {{ to_timestamp }}',
        time_not_in_range_filter: '{{ column }} < {{ from_timestamp }} OR {{ column }} > {{ to_timestamp }}',
        gt: '{{ column }} > {{ param }}',
        gte: '{{ column }} >= {{ param }}',
        lt: '{{ column }} < {{ param }}',
        lte: '{{ column }} <= {{ param }}',
        like_pattern: '{% if start_wild %}\'%\' || {% endif %}{{ value }}{% if end_wild %}|| \'%\'{% endif %}',
        always_true: '1 = 1'

      },
      operators: {},
      quotes: {
        identifiers: '"',
        escape: '""'
      },
      params: {
        param: '?'
      },
      join_types: {
        inner: 'INNER',
        left: 'LEFT',
      },
      window_frame_types: {
        rows: 'ROWS',
        range: 'RANGE',
      },
      window_frame_bounds: {
        preceding: '{% if n is not none %}{{ n }}{% else %}UNBOUNDED{% endif %} PRECEDING',
        current_row: 'CURRENT ROW',
        following: '{% if n is not none %}{{ n }}{% else %}UNBOUNDED{% endif %} FOLLOWING',
      },
      types: {
        string: 'STRING',
        boolean: 'BOOLEAN',
        tinyint: 'TINYINT',
        smallint: 'SMALLINT',
        integer: 'INTEGER',
        bigint: 'BIGINT',
        float: 'FLOAT',
        double: 'DOUBLE',
        decimal: 'DECIMAL({{ precision }},{{ scale }})',
        timestamp: 'TIMESTAMP',
        date: 'DATE',
        time: 'TIME',
        interval: 'INTERVAL',
        binary: 'BINARY',
      },
    };
  }

  /**
   *
   * @param cube
   * @param preAggregation
   * @returns {BaseQuery}
   */
  // eslint-disable-next-line consistent-return
  preAggregationQueryForSqlEvaluation(cube, preAggregation, context = {}) {
    if (preAggregation.type === 'autoRollup') {
      return this.preAggregations.autoRollupPreAggregationQuery(cube, preAggregation);
    } else if (preAggregation.type === 'rollup') {
      return this.preAggregations.rollupPreAggregationQuery(cube, preAggregation, context);
    } else if (preAggregation.type === 'originalSql') {
      return this;
    }
  }

  parseCronSyntax(every) {
    // One of the years that start from monday (first day of week)
    // Mon, 01 Jan 2018 00:00:00 GMT
    const startDate = 1514764800000;
    const opt = {
      utc: true,
      currentDate: new Date(startDate)
    };

    try {
      const interval = cronParser.parseExpression(every, opt);
      let dayOffset = interval.next().getTime();
      const dayOffsetPrev = interval.prev().getTime();

      if (dayOffsetPrev === startDate) {
        dayOffset = startDate;
      }

      return {
        start: interval.next(),
        end: interval.next(),
        dayOffset: (dayOffset - startDate) / 1000,
      };
    } catch (err) {
      throw new UserError(`Invalid cron string '${every}' in refreshKey (${err})`);
    }
  }

  calcIntervalForCronString(refreshKey) {
    const every = refreshKey.every || '1 hour';

    const { start, end, dayOffset } = this.parseCronSyntax(every);

    const interval = (end.getTime() - start.getTime()) / 1000;

    if (
      !/^(\*|\d+)? ?(\*|\d+) (\*|\d+) \* \* (\*|\d+)$/g.test(every.replace(/ +/g, ' ').replace(/^ | $/g, ''))
    ) {
      throw new UserError(`Your cron string ('${every}') is correct, but we support only equal time intervals.`);
    }

    let utcOffset = 0;

    if (refreshKey.timezone || this.timezone) {
      utcOffset = moment.tz(refreshKey.timezone).utcOffset() * 60;
    }

    return {
      utcOffset,
      interval,
      dayOffset,
    };
  }

  everyRefreshKeySql(refreshKey, external = false) {
    if (this.externalQueryClass) {
      return this.externalQuery().everyRefreshKeySql(refreshKey, true);
    }

    const every = refreshKey.every || '1 hour';

    if (/^(\d+) (second|minute|hour|day|week)s?$/.test(every)) {
      const utcOffset = this.timezone ? moment.tz(this.timezone).utcOffset() * 60 : 0;
      const utcOffsetPrefix = utcOffset ? `${utcOffset} + ` : '';
      return [this.floorSql(`(${utcOffsetPrefix}${this.unixTimestampSql()}) / ${this.parseSecondDuration(every)}`), external, this];
    }

    const { dayOffset, utcOffset, interval } = this.calcIntervalForCronString(refreshKey);

    /**
     * Small explanation how it works for every `0 8 * * *`
     * 28800 is a $dayOffset
     *
     * SELECT ((3600 * 8 - 28800) / 86400); -- 0
     * SELECT ((3600 * 16 - 28800) / 86400); -- 0
     * SELECT ((3600 * 24 - 28800) / 86400); -- 0
     * SELECT ((3600 * (24 + 8) - 28800) / 86400); -- 1
     * SELECT ((3600 * (48 + 8) - 28800) / 86400); -- 2
     */
    return [this.floorSql(`(${utcOffset} + ${this.unixTimestampSql()} - ${dayOffset}) / ${interval}`), external, this];
  }

  granularityFor(momentDate) {
    const obj = momentDate.toObject();
    const weekDay = momentDate.isoWeekday();
    if (
      obj.months === 0 &&
      obj.date === 1 &&
      obj.hours === 0 &&
      obj.minutes === 0 &&
      obj.seconds === 0 &&
      obj.milliseconds === 0
    ) {
      return 'year';
    } else if (
      obj.date === 1 &&
      obj.hours === 0 &&
      obj.minutes === 0 &&
      obj.seconds === 0 &&
      obj.milliseconds === 0
    ) {
      return 'month';
    } else if (
      weekDay === 1 &&
      obj.hours === 0 &&
      obj.minutes === 0 &&
      obj.seconds === 0 &&
      obj.milliseconds === 0
    ) {
      return 'week';
    } else if (
      obj.hours === 0 &&
      obj.minutes === 0 &&
      obj.seconds === 0 &&
      obj.milliseconds === 0
    ) {
      return 'day';
    } else if (
      obj.minutes === 0 &&
      obj.seconds === 0 &&
      obj.milliseconds === 0
    ) {
      return 'hour';
    } else if (
      obj.seconds === 0 &&
      obj.milliseconds === 0
    ) {
      return 'minute';
    } else if (
      obj.milliseconds === 0
    ) {
      return 'second';
    }
    return 'second'; // TODO return 'millisecond';
  }

  /**
   * @protected
   * @param {string} interval
   * @return {[number, string]}
   */
  parseInterval(interval) {
    const intervalMatch = interval.match(/^(-?\d+) (second|minute|hour|day|week|month|quarter|year)s?$/);
    if (!intervalMatch) {
      throw new UserError(`Invalid interval: ${interval}`);
    }

    const duration = parseInt(intervalMatch[1], 10);

    return [duration, intervalMatch[2]];
  }

  negateInterval(interval) {
    const [duration, grunularity] = this.parseInterval(interval);

    return `${duration * -1} ${grunularity}`;
  }

  parseSecondDuration(interval) {
    const [duration, type] = this.parseInterval(interval);

    const secondsInInterval = SecondsDurations[type];
    return secondsInInterval * duration;
  }

  floorSql(numeric) {
    return `FLOOR(${numeric})`;
  }

  incrementalRefreshKey(query, originalRefreshKey, options = {}) {
    const refreshKeyQuery = options.refreshKeyQuery || query;
    const updateWindow = options.window;
    const timeDimension = query.timeDimensions[0];

    // TODO use timeDimension from refreshKeyQuery directly
    const dateTo = refreshKeyQuery.timeStampCast(refreshKeyQuery.paramAllocator.allocateParam(timeDimension.dateTo()));
    return refreshKeyQuery.caseWhenStatement([{
      sql: `${refreshKeyQuery.nowTimestampSql()} < ${updateWindow ?
        refreshKeyQuery.addTimestampInterval(dateTo, updateWindow) :
        dateTo
      }`,
      label: originalRefreshKey
    }]);
  }

  defaultRefreshKeyRenewalThreshold() {
    return 10;
  }

  defaultEveryRefreshKey() {
    return {
      every: '10 seconds'
    };
  }

  /**
   * Some databases can return dynamically column name, for example Cube Store
   *
   * SELECT FLOOR((UNIX_TIMESTAMP()) / 60);
   * +-------------------------------------------+
   * | floor(Int64(1625395697) Divide Int64(60)) |
   * +-------------------------------------------+
   * | 27089928                                  |
   * +-------------------------------------------+
   * 1 row in set (0.00 sec)
   *
   * @protected
   *
   * @param {string} sql
   * @return {string}
   */
  refreshKeySelect(sql) {
    return `SELECT ${sql} as refresh_key`;
  }

  partitionInvalidateKeyQueries(_cube, _preAggregation) {
    // this is not used across all dialects, atm only in KsqlQuery.
  }

  preAggregationInvalidateKeyQueries(cube, preAggregation, preAggregationName) {
    return this.cacheValue(
      ['preAggregationInvalidateKeyQueries', cube, JSON.stringify(preAggregation)],
      () => {
        const preAggregationQueryForSql = this.preAggregationQueryForSqlEvaluation(cube, preAggregation);
        if (preAggregation.refreshKey) {
          if (preAggregation.refreshKey.sql) {
            return [
              preAggregationQueryForSql.paramAllocator.buildSqlAndParams(
                preAggregationQueryForSql.evaluateSql(cube, preAggregation.refreshKey.sql)
              ).concat({
                external: false,
                renewalThreshold: preAggregation.refreshKey.every
                  ? this.refreshKeyRenewalThresholdForInterval(preAggregation.refreshKey, false)
                  : this.defaultRefreshKeyRenewalThreshold(),
              })
            ];
          }

          // eslint-disable-next-line prefer-const
          let [refreshKey, refreshKeyExternal, refreshKeyQuery] = this.everyRefreshKeySql(preAggregation.refreshKey);
          const renewalThreshold = this.refreshKeyRenewalThresholdForInterval(preAggregation.refreshKey);
          if (preAggregation.refreshKey.incremental) {
            if (!preAggregation.partitionGranularity) {
              throw new UserError(`Incremental refresh key can only be used for partitioned pre-aggregations but set for non-partitioned '${cube}.${preAggregationName}'`);
            }
            // TODO Case when partitioned originalSql is resolved for query without time dimension.
            // Consider fallback to not using such originalSql for consistency?
            if (
              preAggregationQueryForSql.timeDimensions.length &&
              preAggregationQueryForSql.timeDimensions[0].dateRange
            ) {
              refreshKey = this.incrementalRefreshKey(
                preAggregationQueryForSql,
                refreshKey,
                { window: preAggregation.refreshKey.updateWindow, refreshKeyQuery }
              );
            }
          }

          if (preAggregation.refreshKey.every || preAggregation.refreshKey.incremental) {
            return [
              refreshKeyQuery.paramAllocator.buildSqlAndParams(this.refreshKeySelect(refreshKey)).concat({
                external: refreshKeyExternal,
                renewalThreshold,
                incremental: preAggregation.refreshKey.incremental,
                updateWindowSeconds: preAggregation.refreshKey.updateWindow &&
                  this.parseSecondDuration(preAggregation.refreshKey.updateWindow),
                renewalThresholdOutsideUpdateWindow: preAggregation.refreshKey.incremental &&
                  24 * 60 * 60
              })
            ];
          }
        }

        if (preAggregation.type === 'originalSql') {
          return this.evaluateSymbolSqlWithContext(
            () => this.refreshKeysByCubes([cube]),
            { preAggregationQuery: true }
          );
        }

        if (
          !preAggregationQueryForSql.allCubeNames.find(c => {
            const fromPath = this.cubeEvaluator.cubeFromPath(c);
            return fromPath.refreshKey && fromPath.refreshKey.sql;
          })
        ) {
          const cubeFromPath = this.cubeEvaluator.cubeFromPath(cube);
          return preAggregationQueryForSql.evaluateSymbolSqlWithContext(
            () => preAggregationQueryForSql.cacheKeyQueries(
              (refreshKeyCube, [refreshKeySQL, refreshKeyQueryOptions, refreshKeyQuery]) => {
                if (!cubeFromPath.refreshKey) {
                  const [sql, external, query] = this.everyRefreshKeySql({
                    every: '1 hour'
                  });

                  return [
                    this.refreshKeySelect(sql),
                    {
                      external,
                      renewalThreshold: this.defaultRefreshKeyRenewalThreshold(),
                    },
                    query
                  ];
                }

                return [refreshKeySQL, refreshKeyQueryOptions, refreshKeyQuery];
              }
            ),
            { preAggregationQuery: true }
          );
        }

        return preAggregationQueryForSql.evaluateSymbolSqlWithContext(
          () => preAggregationQueryForSql.cacheKeyQueries(),
          { preAggregationQuery: true }
        );
      },
      { inputProps: { collectOriginalSqlPreAggregations: [] }, cache: this.queryCache }
    );
  }

  refreshKeyRenewalThresholdForInterval(refreshKey, everyWithoutSql = true) {
    const { every } = refreshKey;

    if (/^(\d+) (second|minute|hour|day|week)s?$/.test(every)) {
      const threshold = Math.max(Math.round(this.parseSecondDuration(every) / (everyWithoutSql ? 10 : 1)), 1);

      if (everyWithoutSql) {
        return Math.min(threshold, 300);
      }

      return threshold;
    }

    const { interval } = this.calcIntervalForCronString(refreshKey);
    const threshold = Math.max(Math.round(interval / (everyWithoutSql ? 10 : 1)), 1);

    if (everyWithoutSql) {
      return Math.min(threshold, 300);
    }

    return threshold;
  }

  preAggregationStartEndQueries(cube, preAggregation) {
    const references = this.cubeEvaluator.evaluatePreAggregationReferences(cube, preAggregation);
    const timeDimension = this.newTimeDimension(references.timeDimensions[0]);

    return this.evaluateSymbolSqlWithContext(() => [
      this.paramAllocator.buildSqlAndParams(
        preAggregation.refreshRangeStart && this.evaluateSql(cube, preAggregation.refreshRangeStart.sql) ||
        this.aggSelectForDimension(timeDimension.path()[0], timeDimension, 'min')
      ),
      this.paramAllocator.buildSqlAndParams(
        preAggregation.refreshRangeEnd && this.evaluateSql(cube, preAggregation.refreshRangeEnd.sql) ||
        this.aggSelectForDimension(timeDimension.path()[0], timeDimension, 'max')
      )
    ], { preAggregationQuery: true });
  }

  parametrizedContextSymbols() {
    if (!this.parametrizedContextSymbolsValue) {
      this.parametrizedContextSymbolsValue = Object.assign({
        filterParams: this.filtersProxy(),
        filterGroup: this.filterGroupFunction(),
        sqlUtils: {
          convertTz: this.convertTz.bind(this)
        }
      }, R.map(
        (symbols) => this.contextSymbolsProxy(symbols),
        this.contextSymbols
      ));
    }
    return this.parametrizedContextSymbolsValue;
  }

  static emptyParametrizedContextSymbols(cubeEvaluator, allocateParam) {
    return {
      filterParams: BaseQuery.filterProxyFromAllFilters(null, cubeEvaluator, allocateParam, (filter) => new BaseGroupFilter(filter)),
      filterGroup: () => '1 = 1',
      sqlUtils: {
        convertTz: (field) => field,
      },
      securityContext: CubeSymbols.contextSymbolsProxyFrom({}, allocateParam),
    };
  }

  securityContextForRust() {
    return this.contextSymbolsProxy(this.contextSymbols.securityContext);
  }

  sqlUtilsForRust() {
    return {
      convertTz: this.convertTz.bind(this)
    };
  }

  contextSymbolsProxy(symbols) {
    return CubeSymbols.contextSymbolsProxyFrom(symbols, this.paramAllocator.allocateParam.bind(this.paramAllocator));
  }

  static extractFilterMembers(filter) {
    if (filter.operator === 'and' || filter.operator === 'or') {
      return filter.values.map(f => BaseQuery.extractFilterMembers(f)).reduce((a, b) => ((a && b) ? { ...a, ...b } : null), {});
    } else if (filter.measure) {
      return {
        [filter.measure]: true
      };
    } else if (filter.dimension) {
      return {
        [filter.dimension]: true
      };
    } else {
      return null;
    }
  }

  static findAndSubTreeForFilterGroup(filter, groupMembers, newGroupFilter, aliases) {
    if ((filter.operator === 'and' || filter.operator === 'or') && !filter.values?.length) {
      return null;
    }
    const filterMembers = BaseQuery.extractFilterMembers(filter);
    if (filterMembers && Object.keys(filterMembers).every(m => (groupMembers.indexOf(m) !== -1 || aliases.indexOf(m) !== -1))) {
      return filter;
    }
    if (filter.operator === 'and') {
      const result = filter.values.map(f => BaseQuery.findAndSubTreeForFilterGroup(f, groupMembers, newGroupFilter, aliases)).filter(f => !!f);
      if (!result.length) {
        return null;
      }
      if (result.length === 1) {
        return result[0];
      }
      return newGroupFilter({
        operator: 'and',
        values: result
      });
    }
    return null;
  }

  filtersProxy() {
    const { allFilters } = this;
    return BaseQuery.filterProxyFromAllFilters(
      allFilters,
      this.cubeEvaluator,
      this.paramAllocator.allocateParam.bind(this.paramAllocator),
      this.newGroupFilter.bind(this),
    );
  }

  filtersProxyForRust(usedFilters) {
    const filters = this.extractFiltersAsTree(usedFilters || []);
    const allFilters = filters.map(this.initFilter.bind(this));
    return BaseQuery.filterProxyFromAllFilters(
      allFilters,
      this.cubeEvaluator,
      this.paramAllocator.allocateParam.bind(this.paramAllocator),
      this.newGroupFilter.bind(this),
    );
  }

  filterGroupFunctionForRust(usedFilters) {
    const filters = this.extractFiltersAsTree(usedFilters || []);
    const allFilters = filters.map(this.initFilter.bind(this));
    return this.filterGroupFunctionImpl(allFilters);
  }

  static renderFilterParams(filter, filterParamArgs, allocateParam, newGroupFilter, aliases) {
    if (!filter) {
      return BaseFilter.ALWAYS_TRUE;
    }

    if (filter.operator === 'and' || filter.operator === 'or') {
      const values = filter.values
        .map(f => BaseQuery.renderFilterParams(f, filterParamArgs, allocateParam, newGroupFilter, aliases))
        .map(v => ({ filterToWhere: () => v }));

      return newGroupFilter({ operator: filter.operator, values }).filterToWhere();
    }

    const filterParams = filter.filterParams();
    const filterParamArg = filterParamArgs.filter(p => {
      const member = p.__member();
      return member === filter.measure ||
        member === filter.dimension ||
        (aliases[member] && (
          aliases[member] === filter.measure ||
          aliases[member] === filter.dimension
        ));
    })[0];

    if (!filterParamArg) {
      throw new Error(`FILTER_PARAMS arg not found for ${filter.measure || filter.dimension}`);
    }

    if (typeof filterParamArg.__column() !== 'function') {
      return filter.conditionSql(filterParamArg.__column());
    }

    if (!filterParams || !filterParams.length) {
      return BaseFilter.ALWAYS_TRUE;
    }

    // eslint-disable-next-line prefer-spread
    return filterParamArg.__column().apply(
      null,
      filterParams.map(allocateParam),
    );
  }

  filterGroupFunction() {
    const { allFilters } = this;
    return this.filterGroupFunctionImpl(allFilters);
  }

  filterGroupFunctionImpl(allFilters) {
    const allocateParam = this.paramAllocator.allocateParam.bind(this.paramAllocator);
    const newGroupFilter = this.newGroupFilter.bind(this);
    return (...filterParamArgs) => {
      const groupMembers = filterParamArgs.map(f => {
        if (!f.__member) {
          throw new UserError(`FILTER_GROUP expects FILTER_PARAMS args to be passed. For example FILTER_GROUP(FILTER_PARAMS.foo.bar.filter('bar'), FILTER_PARAMS.foo.jar.filter('jar')). But found: ${f}`);
        }
        return f.__member();
      });

      const aliases = allFilters ?
        allFilters
          .map(v => (v.query ? v.query.allBackAliasMembersExceptSegments() : {}))
          .reduce((a, b) => ({ ...a, ...b }), {})
        : {};
      // Filtering aliases that somehow relate to this group members
      const aliasesForGroupMembers = Object.entries(aliases)
        .filter(([key, value]) => groupMembers.includes(key))
        .map(([_key, value]) => value);
      const filter = BaseQuery.findAndSubTreeForFilterGroup(
        newGroupFilter({ operator: 'and', values: allFilters }),
        groupMembers,
        newGroupFilter,
        aliasesForGroupMembers
      );

      return `(${BaseQuery.renderFilterParams(filter, filterParamArgs, allocateParam, newGroupFilter, aliases)})`;
    };
  }

  static filterProxyFromAllFilters(allFilters, cubeEvaluator, allocateParam, newGroupFilter) {
    return new Proxy({}, {
      get: (_target, name) => {
        if (name === '_objectWithResolvedProperties') {
          return true;
        }
        // allFilters is null whenever it's used to test if the member is owned by cube so it should always render to `1 = 1`
        // and do not check cube validity as it's part of compilation step.
        const cubeName = allFilters && cubeEvaluator.cubeNameFromPath(name);
        return new Proxy({ cube: cubeName }, {
          get: (cubeNameObj, propertyName) => ({
            filter: (column) => ({
              __column() {
                return column;
              },
              __member() {
                return cubeEvaluator.pathFromArray([cubeNameObj.cube, propertyName]);
              },
              toString() {
                // Segments should be excluded because they are evaluated separately in cubeReferenceProxy
                // In other case this falls into the recursive loop/stack exceeded caused by:
                // collectFrom() -> traverseSymbol() -> evaluateSymbolSql() ->
                // evaluateSql() -> resolveSymbolsCall() -> cubeReferenceProxy->toString() ->
                // evaluateSymbolSql() -> evaluateSql()... -> and got here again
                //
                // When FILTER_PARAMS is used in dimension/measure SQL - we also hit recursive loop:
                // allBackAliasMembersExceptSegments() -> collectFrom() -> traverseSymbol() -> evaluateSymbolSql() ->
                // autoPrefixAndEvaluateSql() -> evaluateSql() -> filterProxyFromAllFilters->Proxy->toString()
                // and so on...
                // For this case aliasGathering flag is added to the context in first iteration and
                // is checked below to prevent looping.
                const aliases = allFilters ?
                  allFilters
                    .map(v => (v.query && !v.query.safeEvaluateSymbolContext().aliasGathering ? v.query.allBackAliasMembersExceptSegments() : {}))
                    .reduce((a, b) => ({ ...a, ...b }), {})
                  : {};
                // Filtering aliases that somehow relate to this group member
                const groupMember = cubeEvaluator.pathFromArray([cubeNameObj.cube, propertyName]);
                const aliasesForGroupMembers = Object.entries(aliases)
                  .filter(([key, _value]) => key === groupMember)
                  .map(([_key, value]) => value);
                const filter = BaseQuery.findAndSubTreeForFilterGroup(
                  newGroupFilter({ operator: 'and', values: allFilters }),
                  [groupMember],
                  newGroupFilter,
                  aliasesForGroupMembers
                );

                return `(${BaseQuery.renderFilterParams(filter, [this], allocateParam, newGroupFilter, aliases)})`;
              }
            })
          })
        });
      }
    });
  }

  /**
   *
   * @param {boolean} excludeSegments
   * @returns {Array<BaseMeasure | BaseDimension | BaseSegment>}
   */
  flattenAllMembers(excludeSegments = false) {
    return R.flatten(
      this.measures
        .concat(this.dimensions)
        .concat(excludeSegments ? [] : this.segments)
        .concat(this.filters)
        .concat(this.measureFilters)
        .concat(this.timeDimensions)
        .map(m => m.getMembers()),
    );
  }

  /**
   * @returns {Record<string, string>}
   */
  allBackAliasTimeDimensions() {
    const members = R.flatten(this.timeDimensions.map(m => m.getMembers()));
    return this.backAliasMembers(members);
  }

  /**
   * @returns {Record<string, string>}
   */
  allBackAliasMembersExceptSegments() {
    return this.backAliasMembers(this.flattenAllMembers(true));
  }

  /**
   * @returns {Record<string, string>}
   */
  allBackAliasMembers() {
    return this.backAliasMembers(this.flattenAllMembers());
  }

  /**
   *
   * @param {Array<BaseMeasure | BaseDimension | BaseSegment>} members
   * @returns {Record<string, string>}
   */
  backAliasMembers(members) {
    const query = this;

    const aliases = Object.fromEntries(members.flatMap(
      member => {
        const collectedMembers = query.evaluateSymbolSqlWithContext(
          () => query.collectFrom([member], query.collectMemberNamesFor.bind(query), 'collectMemberNamesFor'),
          { aliasGathering: true }
        );
        const memberPath = member.expressionPath();
        let nonAliasSeen = false;
        return collectedMembers
          .filter(d => {
            if (!query.cubeEvaluator.byPathAnyType(d).aliasMember) {
              nonAliasSeen = true;
            }
            return !nonAliasSeen;
          })
          .map(d => [query.cubeEvaluator.byPathAnyType(d).aliasMember, memberPath]);
      }
    ));

    // No join/graph  might be in place when collecting members from the query with some injected filters,
    // like FILTER_PARAMS or securityContext...
    // So we simply return aliases as is
    if (!this.join || !this.joinGraphPaths) {
      return aliases;
    }

    const buildJoinPath = this.buildJoinPathFn();

    /**
     * @type {Record<string, string>}
     */
    const res = {};
    for (const [original, alias] of Object.entries(aliases)) {
      const [cube, field] = original.split('.');
      const path = buildJoinPath(cube);

      const [aliasCube, aliasField] = alias.split('.');
      const aliasPath = aliasCube !== cube ? buildJoinPath(aliasCube) : path;

      if (path) {
        res[`${path}.${field}`] = aliasPath ? `${aliasPath}.${aliasField}` : alias;
      }

      // Aliases might come from proxied members, in such cases
      // we need to map them to originals too
      if (aliasPath) {
        res[original] = `${aliasPath}.${aliasField}`;
      }
    }

    return res;
  }

  buildJoinPathFn() {
    const query = this;
    const { root } = this.join || {};

    return (target) => {
      const visited = new Set();
      const path = [];

      /**
       * @param {string} node
       * @returns {boolean}
       */
      function dfs(node) {
        if (node === target) {
          path.push(node);
          return true;
        }

        if (visited.has(node)) return false;
        visited.add(node);

        const neighbors = query.joinGraphPaths[node] || [];
        for (const neighbor of neighbors) {
          if (dfs(neighbor)) {
            path.unshift(node);
            return true;
          }
        }

        return false;
      }

      return dfs(root) ? path.join('.') : null;
    };
  }

  /**
   * Returns a function that constructs the full member path
   * based on the query's join structure.
   * @returns {(function(member: string): (string))}
   */
  resolveFullMemberPathFn() {
    const { root: queryJoinRoot } = this.join || {};

    const buildJoinPath = this.buildJoinPathFn();

    return (member) => {
      const [cube, field] = member.split('.');
      if (!cube || !field) return member;

      if (cube === queryJoinRoot.root) {
        return member;
      }

      const path = buildJoinPath(cube);
      return path ? `${path}.${field}` : member;
    };
  }
}
