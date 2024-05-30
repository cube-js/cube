/* eslint-disable no-unused-vars,prefer-template */

/**
 * @fileoverview BaseQuery class definition.
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

import R from 'ramda';
import cronParser from 'cron-parser';

import moment from 'moment-timezone';
import inflection from 'inflection';
import { FROM_PARTITION_RANGE, inDbTimeZone, MAX_SOURCE_ROW_LIMIT, QueryAlias } from '@cubejs-backend/shared';

import { UserError } from '../compiler/UserError';
import { BaseMeasure } from './BaseMeasure';
import { BaseDimension } from './BaseDimension';
import { BaseSegment } from './BaseSegment';
import { BaseFilter } from './BaseFilter';
import { BaseGroupFilter } from './BaseGroupFilter';
import { BaseTimeDimension } from './BaseTimeDimension';
import { ParamAllocator } from './ParamAllocator';
import { PreAggregations } from './PreAggregations';
import { SqlParser } from '../parser/SqlParser';

const DEFAULT_PREAGGREGATIONS_SCHEMA = 'stb_pre_aggregations';

const standardGranularitiesParents = {
  year: ['year', 'quarter', 'month', 'month', 'day', 'hour', 'minute', 'second'],
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
      externalClassName: this.options.externalQueryClass && this.options.externalQueryClass.name,
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
      postAggregateQuery: this.options.postAggregateQuery,
      postAggregateDimensions: this.options.postAggregateDimensions,
      postAggregateTimeDimensions: this.options.postAggregateTimeDimensions,
    });
    this.from = this.options.from;
    this.postAggregateQuery = this.options.postAggregateQuery;
    this.timezone = this.options.timezone;
    this.rowLimit = this.options.rowLimit;
    this.offset = this.options.offset;
    this.preAggregations = this.newPreAggregations();
    this.measures = (this.options.measures || []).map(this.newMeasure.bind(this));
    this.dimensions = (this.options.dimensions || []).map(this.newDimension.bind(this));
    this.postAggregateDimensions = (this.options.postAggregateDimensions || []).map(this.newDimension.bind(this));
    this.postAggregateTimeDimensions = (this.options.postAggregateTimeDimensions || []).map(this.newTimeDimension.bind(this));
    this.segments = (this.options.segments || []).map(this.newSegment.bind(this));
    this.order = this.options.order || [];
    const filters = this.extractFiltersAsTree(this.options.filters || []);

    // measure_filter (the one extracted from filters parameter on measure and
    // used in drill downs) should go to WHERE instead of HAVING
    /** @type {(BaseFilter|BaseGroupFilter)[]} */
    this.filters = filters.filter(f => f.dimensionGroup || f.dimension || f.operator === 'measure_filter' || f.operator === 'measureFilter').map(this.initFilter.bind(this));
    this.measureFilters = filters.filter(f => (f.measureGroup || f.measure) && f.operator !== 'measure_filter' && f.operator !== 'measureFilter').map(this.initFilter.bind(this));
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

    this.join = this.joinGraph.buildJoin(this.allJoinHints);
    this.cubeAliasPrefix = this.options.cubeAliasPrefix;
    this.preAggregationsSchemaOption =
      this.options.preAggregationsSchema != null ? this.options.preAggregationsSchema : DEFAULT_PREAGGREGATIONS_SCHEMA;

    if (this.order.length === 0) {
      this.order = this.defaultOrder();
    }

    this.externalQueryClass = this.options.externalQueryClass;
    this.initUngrouped();
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

  get allJoinHints() {
    if (!this.collectedJoinHints) {
      this.collectedJoinHints = this.collectJoinHints();
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
      if (!this.options.allowUngroupedWithoutPrimaryKey) {
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
    if (this.options.preAggregationQuery) {
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

      let id = firstMeasure.measure;

      if (firstMeasure.expressionName) {
        id = firstMeasure.expressionName;
      }

      res.push({ id, desc: true });
    } else if (this.dimensions.length > 0) {
      res.push({
        id: this.dimensions[0].dimension,
        desc: false,
      });
    }

    return res;
  }

  newMeasure(measurePath) {
    return new BaseMeasure(this, measurePath);
  }

  newDimension(dimensionPath) {
    return new BaseDimension(this, dimensionPath);
  }

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

  newParamAllocator(expressionParams) {
    return new ParamAllocator(expressionParams);
  }

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
    if (!this.options.preAggregationQuery) {
      preAggForQuery =
        this.preAggregations.findPreAggregationForQuery();
      if (this.options.disableExternalPreAggregations && preAggForQuery && preAggForQuery.preAggregation.external) {
        preAggForQuery = undefined;
      }
    }
    if (preAggForQuery) {
      const {
        multipliedMeasures,
        regularMeasures,
        cumulativeMeasures,
        withQueries,
        postAggregateMembers,
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
    return `select count(*) ${
      this.escapeColumnName(QueryAlias.TOTAL_COUNT)
    } from (\n${
      sql
    }\n) ${
      this.escapeColumnName(QueryAlias.ORIGINAL_QUERY)
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
      if (preAggregationForQuery && preAggregationForQuery.preAggregation.external) {
        return true;
      }
      const preAggregationsDescription = this.preAggregations.preAggregationsDescription();

      return preAggregationsDescription.length > 0 && R.all((p) => p.external, preAggregationsDescription);
    }

    return false;
  }

  /**
   * Returns an array of SQL query strings for the query.
   * @param {boolean} [exportAnnotatedSql] - returns annotated sql with not rendered params if true
   * @returns {Array<string>}
   */
  buildSqlAndParams(exportAnnotatedSql) {
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
    return this.timeDimensions.map(
      d => [
        d,
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        (dateFrom, dateTo, dateField, dimensionDateFrom, dimensionDateTo) => `${dateField} >= ${dimensionDateFrom} AND ${dateField} <= ${dateTo}`
      ]
    );
  }

  rollingWindowToDateJoinCondition(granularity) {
    return this.timeDimensions.map(
      d => [
        d,
        (dateFrom, dateTo, dateField, dimensionDateFrom, dimensionDateTo, isFromStartToEnd) => `${dateField} >= ${this.timeGroupedColumn(granularity, dateFrom)} AND ${dateField} <= ${dateTo}`
      ]
    );
  }

  rollingWindowDateJoinCondition(trailingInterval, leadingInterval, offset) {
    offset = offset || 'end';
    return this.timeDimensions.map(
      d => [d, (dateFrom, dateTo, dateField, dimensionDateFrom, dimensionDateTo, isFromStartToEnd) => {
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

  subtractInterval(date, interval) {
    return `${date} - interval '${interval}'`;
  }

  addInterval(date, interval) {
    return `${date} + interval '${interval}'`;
  }

  addTimestampInterval(timestamp, interval) {
    return this.addInterval(timestamp, interval);
  }

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
    if (this.postAggregateQuery) {
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
      postAggregateMembers,
    } = this.fullKeyQueryAggregateMeasures();

    if (!multipliedMeasures.length && !cumulativeMeasures.length && !postAggregateMembers.length) {
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
                `${
                  this.aliasName(measure.measure.replace('.', '_'))
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
          ).concat(postAggregateMembers.map(m => `SELECT * FROM ${m.alias}`));
    }

    // Move regular measures to multiplied ones if there're same
    // cubes to calculate. Most of the times it'll be much faster to
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

    const postAggregateMeasures = R.flatten(postAggregateMembers.map(m => m.measures)).map(m => this.newMeasure(m));

    return this.withQueries(this.joinFullKeyQueryAggregate(
      // TODO separate param?
      multipliedMeasures.concat(postAggregateMeasures),
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
        R.map(m => [m.measure || m.dimension, m.aliasName()]),
        R.fromPairs,
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
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
    const measureToHierarchy = this.collectRootMeasureToHieararchy(context);
    const allMemberChildren = this.collectAllMemberChildren(context);
    const memberToIsPostAggregate = this.collectAllPostAggregateMembers(allMemberChildren);

    const hasPostAggregateMembers = (m) => {
      if (memberToIsPostAggregate[m]) {
        return true;
      }
      return allMemberChildren[m]?.some(c => hasPostAggregateMembers(c)) || false;
    };

    const measuresToRender = (multiplied, cumulative) => R.pipe(
      R.values,
      R.flatten,
      R.filter(
        m => m.multiplied === multiplied && this.newMeasure(m.measure).isCumulative() === cumulative && !hasPostAggregateMembers(m.measure)
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
    const postAggregateMembers = R.uniq(
      this.allMembersConcat(false)
        // TODO boolean logic filter support
        .filter(m => m.expressionPath && hasPostAggregateMembers(m.expressionPath()))
        .map(m => m.expressionPath())
    ).map(m => this.postAggregateWithQueries(
      m,
      {
        dimensions: this.dimensions.map(d => d.dimension),
        postAggregateDimensions: this.dimensions.map(d => d.dimension),
        // TODO accessing timeDimensions directly from options might miss some processing logic
        timeDimensions: this.options.timeDimensions || [],
        postAggregateTimeDimensions: (this.options.timeDimensions || []).filter(td => !!td.granularity),
        // TODO accessing filters directly from options might miss some processing logic
        filters: this.options.filters || []
      },
      allMemberChildren,
      withQueries
    ));
    const usedWithQueries = {};
    postAggregateMembers.forEach(m => this.collectUsedWithQueries(usedWithQueries, m));

    return {
      multipliedMeasures,
      regularMeasures,
      cumulativeMeasures,
      postAggregateMembers,
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

  collectAllPostAggregateMembers(allMemberChildren) {
    const allMembers = R.uniq(R.flatten(Object.keys(allMemberChildren).map(k => [k].concat(allMemberChildren[k]))));
    return R.fromPairs(allMembers.map(m => ([m, this.memberInstanceByPath(m).isPostAggregate()])));
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

  postAggregateWithQueries(member, queryContext, memberChildren, withQueries) {
    // TODO calculate based on remove_filter in future
    const wouldNodeApplyFilters = !memberChildren[member];
    let memberFrom = memberChildren[member]
      ?.map(child => this.postAggregateWithQueries(child, this.childrenPostAggregateContext(member, queryContext, wouldNodeApplyFilters), memberChildren, withQueries));
    const unionFromDimensions = memberFrom ? R.uniq(R.flatten(memberFrom.map(f => f.dimensions))) : queryContext.dimensions;
    const unionDimensionsContext = { ...queryContext, dimensions: unionFromDimensions.filter(d => !this.newDimension(d).isPostAggregate()) };
    // TODO is calling postAggregateWithQueries twice optimal?
    memberFrom = memberChildren[member] &&
      R.uniqBy(
        f => f.alias,
        memberChildren[member].map(child => this.postAggregateWithQueries(child, this.childrenPostAggregateContext(member, unionDimensionsContext, wouldNodeApplyFilters), memberChildren, withQueries))
      );
    const selfContext = this.selfPostAggregateContext(member, queryContext, wouldNodeApplyFilters);
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

  childrenPostAggregateContext(memberPath, queryContext, wouldNodeApplyFilters) {
    let member;
    if (this.cubeEvaluator.isMeasure(memberPath)) {
      member = this.newMeasure(memberPath);
    } else if (this.cubeEvaluator.isDimension(memberPath)) {
      member = this.newDimension(memberPath);
    }
    const memberDef = member.definition();
    // TODO can addGroupBy replaced by something else?
    if (memberDef.addGroupByReferences) {
      queryContext = { ...queryContext, dimensions: R.uniq(queryContext.dimensions.concat(memberDef.addGroupByReferences)) };
    }
    if (memberDef.timeShiftReferences) {
      queryContext = {
        ...queryContext,
        timeDimensions: queryContext.timeDimensions.map(td => {
          const timeShift = memberDef.timeShiftReferences.find(r => r.timeDimension === td.dimension);
          if (timeShift) {
            if (td.shiftInterval) {
              throw new UserError(`Hierarchical time shift is not supported but was provided for '${td.dimension}'. Parent time shift is '${td.shiftInterval}' and current is '${timeShift.interval}'`);
            }
            return {
              ...td,
              shiftInterval: timeShift.type === 'next' ? this.negateInterval(timeShift.interval) : timeShift.interval
            };
          }
          return td;
        })
      };
    }
    queryContext = {
      ...queryContext,
      // TODO can't remove filters from OR expression
      filters: this.keepFilters(queryContext.filters, filterMember => filterMember !== memberPath),
    };
    return queryContext;
  }

  selfPostAggregateContext(memberPath, queryContext, wouldNodeApplyFilters) {
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
        postAggregateDimensions: R.difference(queryContext.postAggregateDimensions, memberDef.reduceByReferences),
        postAggregateTimeDimensions: queryContext.postAggregateTimeDimensions.filter(td => memberDef.reduceByReferences.indexOf(td.dimension) === -1),
        // dimensions: R.uniq(queryContext.dimensions.concat(memberDef.reduceByReferences))
      };
    }
    if (memberDef.groupByReferences) {
      queryContext = {
        ...queryContext,
        postAggregateDimensions: R.intersection(queryContext.postAggregateDimensions, memberDef.groupByReferences),
        postAggregateTimeDimensions: queryContext.postAggregateTimeDimensions.filter(td => memberDef.groupByReferences.indexOf(td.dimension) !== -1),
      };
    }
    if (!wouldNodeApplyFilters) {
      queryContext = {
        ...queryContext,
        // TODO make it same way as keepFilters
        timeDimensions: queryContext.timeDimensions.map(td => ({ ...td, dateRange: undefined })),
        filters: this.keepFilters(queryContext.filters, filterMember => filterMember === memberPath),
      };
    } else {
      queryContext = {
        ...queryContext,
        filters: this.keepFilters(queryContext.filters, filterMember => !this.memberInstanceByPath(filterMember).isPostAggregate()),
      };
    }
    return queryContext;
  }

  renderWithQuery(withQuery) {
    const fromMeasures = withQuery.memberFrom && R.uniq(R.flatten(withQuery.memberFrom.map(f => f.measures)));
    // TODO get rid of this postAggregate filter
    const fromDimensions = withQuery.memberFrom && R.uniq(R.flatten(withQuery.memberFrom.map(f => f.dimensions)));
    const fromTimeDimensions = withQuery.memberFrom && R.uniq(R.flatten(withQuery.memberFrom.map(f => (f.timeDimensions || []).map(td => ({ ...td, dateRange: undefined })))));
    const renderedReferenceContext = {
      renderedReference: withQuery.memberFrom && R.fromPairs(
        R.unnest(withQuery.memberFrom.map(from => from.measures.map(m => {
          const measure = this.newMeasure(m);
          return [m, measure.aliasName()];
        }).concat(from.dimensions.map(m => {
          const member = this.newDimension(m);
          return [m, member.aliasName()];
        })).concat(from.timeDimensions.map(m => {
          const member = this.newTimeDimension(m);
          return member.granularity ? [`${member.dimension}.${member.granularity}`, member.aliasName()] : [];
        }))))
      )
    };

    const fromSubQuery = fromMeasures && this.newSubQuery({
      measures: fromMeasures,
      // TODO get rid of this postAggregate filter
      dimensions: fromDimensions, // .filter(d => !this.newDimension(d).isPostAggregate()),
      timeDimensions: fromTimeDimensions,
      postAggregateDimensions: withQuery.postAggregateDimensions,
      postAggregateTimeDimensions: withQuery.postAggregateTimeDimensions,
      filters: withQuery.filters,
      // TODO do we need it?
      postAggregateQuery: true, // !!fromDimensions.find(d => this.newDimension(d).isPostAggregate())
      disableExternalPreAggregations: true,
    });

    const measures = fromSubQuery && fromMeasures.map(m => fromSubQuery.newMeasure(m));
    // TODO get rid of this postAggregate filter
    const postAggregateDimensions = fromSubQuery && fromDimensions.map(m => fromSubQuery.newDimension(m)).filter(d => d.isPostAggregate());
    const postAggregateTimeDimensions = fromSubQuery && fromTimeDimensions.map(m => fromSubQuery.newTimeDimension(m)).filter(d => d.isPostAggregate());
    // TODO not working yet
    const membersToSelect = measures?.concat(postAggregateDimensions).concat(postAggregateTimeDimensions);
    const select = fromSubQuery && fromSubQuery.outerMeasuresJoinFullKeyQueryAggregate(membersToSelect, membersToSelect, withQuery.memberFrom.map(f => f.alias));
    const fromSql = select && this.wrapInParenthesis(select);

    const subQueryOptions = {
      measures: withQuery.measures,
      dimensions: withQuery.dimensions,
      timeDimensions: withQuery.timeDimensions,
      postAggregateDimensions: withQuery.postAggregateDimensions,
      postAggregateTimeDimensions: withQuery.postAggregateTimeDimensions,
      filters: withQuery.filters,
      from: fromSql && {
        sql: fromSql,
        alias: `${withQuery.alias}_join`,
      },
      // TODO condition should something else instead of rank
      postAggregateQuery: !!withQuery.measures.find(d => {
        const { type } = this.newMeasure(d).definition();
        return type === 'rank' || BaseQuery.isCalculatedMeasureType(type);
      }),
      disableExternalPreAggregations: true,
    };
    const subQuery = this.newSubQuery(subQueryOptions);

    if (!subQuery.from) {
      const allSubQueryMembers = R.flatten(subQuery.collectFromMembers(false, subQuery.collectMemberNamesFor.bind(subQuery), 'collectMemberNamesFor'));
      const postAggregateMember = allSubQueryMembers.find(m => this.memberInstanceByPath(m).isPostAggregate());
      if (postAggregateMember) {
        throw new Error(`Post aggregate member '${postAggregateMember}' lacks FROM clause in sub query: ${JSON.stringify(subQueryOptions)}`);
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
    return R.fromPairs(Object.keys(standardGranularitiesParents).map(k => [k, this.granularityParentHierarchy(k)]));
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
    const cumulativeMeasures = [cumulativeMeasure];
    if (!this.timeDimensions.find(d => d.granularity)) {
      const filters = this.segments.concat(this.filters).concat(this.dateFromStartToEndConditionSql(dateJoinCondition, fromRollup, false));
      return baseQueryFn(cumulativeMeasures, filters, false);
    }
    const dateSeriesSql = this.timeDimensions.map(d => this.dateSeriesSql(d)).join(', ');
    const filters = this.segments.concat(this.filters).concat(this.dateFromStartToEndConditionSql(dateJoinCondition, fromRollup, true));
    const baseQuery = this.groupedUngroupedSelect(
      () => baseQueryFn(cumulativeMeasures, filters),
      cumulativeMeasure.shouldUngroupForCumulative(),
      !cumulativeMeasure.shouldUngroupForCumulative() && this.minGranularity(
        cumulativeMeasure.windowGranularity(), this.timeDimensions.find(d => d.granularity).granularity
      ) || undefined
    );
    const baseQueryAlias = this.cubeAlias('base');
    const dateJoinConditionSql =
      dateJoinCondition.map(
        ([d, f]) => f(
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
      baseQueryAlias
    );
  }

  dateFromStartToEndConditionSql(dateJoinCondition, fromRollup, isFromStartToEnd) {
    return dateJoinCondition.map(
      // TODO these weird conversions to be strict typed for big query.
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

  overTimeSeriesSelect(cumulativeMeasures, dateSeriesSql, baseQuery, dateJoinConditionSql, baseQueryAlias) {
    const forSelect = this.overTimeSeriesForSelect(cumulativeMeasures);
    return `SELECT ${forSelect} FROM ${dateSeriesSql}` +
      ` LEFT JOIN (${baseQuery}) ${this.asSyntaxJoin} ${baseQueryAlias} ON ${dateJoinConditionSql}` +
      this.groupByClause();
  }

  overTimeSeriesForSelect(cumulativeMeasures) {
    return this.dimensions.map(s => s.cumulativeSelectColumns()).concat(this.dateSeriesSelect()).concat(
      cumulativeMeasures.map(s => s.cumulativeSelectColumns()),
    ).filter(c => !!c)
      .join(', ');
  }

  dateSeriesSelect() {
    return this.timeDimensions.map(d => d.dateSeriesSelectColumn());
  }

  /**
   * @param {import('./BaseTimeDimension').BaseTimeDimension} timeDimension
   * @return {string}
   */
  dateSeriesSql(timeDimension) {
    return `(${this.seriesSql(timeDimension)}) ${this.asSyntaxTable} ${timeDimension.dateSeriesAliasName()}`;
  }

  /**
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

  commonQuery() {
    return `SELECT${this.topLimit()}
      ${this.baseSelect()}
    FROM
      ${this.query()}`;
  }

  collectRootMeasureToHieararchy(context) {
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
        throw new UserError(`Subquery dimension ${m.expressionName} should reference at least one measure`);
      }
      return [m.measure, collectedMeasures];
    }));
  }

  query() {
    return this.from && this.joinSql([this.from]) || this.joinQuery(this.join, this.collectFromMembers(
      false,
      this.collectSubQueryDimensionsFor.bind(this),
      'collectSubQueryDimensionsFor'
    ));
  }

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

  joinQuery(join, subQueryDimensions) {
    const subQueryDimensionsByCube = R.groupBy(d => this.cubeEvaluator.cubeNameFromPath(d), subQueryDimensions);
    const joins = join.joins.map(
      j => {
        const [cubeSql, cubeAlias, conditions] = this.rewriteInlineCubeSql(j.originalTo, true);
        return [{
          sql: cubeSql,
          alias: cubeAlias,
          on: `${this.evaluateSql(j.originalFrom, j.join.sql)}${conditions ? ` AND (${conditions})` : ''}`
          // TODO handle the case when sub query referenced by a foreign cube on other side of a join
        }].concat((subQueryDimensionsByCube[j.originalTo] || []).map(d => this.subQueryJoin(d)));
      }
    ).reduce((a, b) => a.concat(b), []);

    const [cubeSql, cubeAlias] = this.rewriteInlineCubeSql(join.root);
    return this.joinSql([
      { sql: cubeSql, alias: cubeAlias },
      ...(subQueryDimensionsByCube[join.root] || []).map(d => this.subQueryJoin(d)),
      ...joins
    ]);
  }

  joinSql(toJoin) {
    const [root, ...rest] = toJoin;
    const joins = rest.map(
      j => `LEFT JOIN ${j.sql} ${this.asSyntaxJoin} ${j.alias} ON ${j.on}`
    );

    return [`${root.sql} ${this.asSyntaxJoin} ${root.alias}`, ...joins].join('\n');
  }

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
    return `SELECT ${this.selectAllDimensionsAndMeasures(measures)} FROM ${
      query
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
      const joinHints = this.collectFrom(measures, this.collectJoinHintsFor.bind(this), 'collectJoinHintsFor');
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
      `${
        this.escapeColumnName(QueryAlias.AGG_SUB_QUERY_KEYS)
      }.${
        pkd.aliasName()
      } = ${
        shouldBuildJoinForMeasureSelect
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

  checkShouldBuildJoinForMeasureSelect(measures, keyCubeName) {
    return measures.map(measure => {
      const cubes = this.collectFrom([measure], this.collectCubeNamesFor.bind(this), 'collectCubeNamesFor');
      const joinHints = this.collectFrom([measure], this.collectJoinHintsFor.bind(this), 'collectJoinHintsFor');
      if (R.any(cubeName => keyCubeName !== cubeName, cubes)) {
        const measuresJoin = this.joinGraph.buildJoin(joinHints);
        if (measuresJoin.multiplicationFactor[keyCubeName]) {
          throw new UserError(
            `'${measure.measure}' references cubes that lead to row multiplication. Please rewrite it using sub query.`
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
    return `SELECT DISTINCT ${this.keysSelect(primaryKeyDimensions)} FROM ${
      query
    } ${this.baseWhere(filters.concat(inlineWhereConditions))}`;
  }

  keysSelect(primaryKeyDimensions) {
    return R.flatten(
      this.keyDimensions(primaryKeyDimensions)
        .map(s => s.selectColumns())
    ).filter(s => !!s).join(', ');
  }

  keyDimensions(primaryKeyDimensions) {
    return R.uniqBy(
      (d) => d.dimension, this.dimensionsForSelect()
        .concat(primaryKeyDimensions)
    );
  }

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
    return s.path() ?
      [s.cube().name].concat(this.evaluateSymbolSql(s.path()[0], s.path()[1], s.definition())) :
      this.evaluateSql(s.cube().name, s.definition().sql);
  }

  collectCubeNames(excludeTimeDimensions) {
    return this.collectFromMembers(
      excludeTimeDimensions,
      this.collectCubeNamesFor.bind(this),
      'collectCubeNamesFor'
    );
  }

  collectJoinHints(excludeTimeDimensions = false) {
    return this.collectFromMembers(
      excludeTimeDimensions,
      this.collectJoinHintsFor.bind(this),
      'collectJoinHintsFor'
    );
  }

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

  allMembersConcat(excludeTimeDimensions) {
    return this.measures
      .concat(this.dimensions)
      .concat(this.segments)
      .concat(this.filters)
      .concat(this.measureFilters)
      .concat(excludeTimeDimensions ? [] : this.timeDimensions);
  }

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

  getFieldIndex(id) {
    const equalIgnoreCase = (a, b) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let index;

    index = this.dimensionsForSelect().findIndex(
      d => equalIgnoreCase(d.dimension, id) || equalIgnoreCase(d.expressionName, id)
    );

    if (index > -1) {
      return index + 1;
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
      if (joinHints.length === 1) {
        [joinHints] = joinHints;
      }
      this.safeEvaluateSymbolContext().joinHints.push(joinHints);
    }
  }

  pushMemberNameForCollectionIfNecessary(cubeName, name) {
    const pathFromArray = this.cubeEvaluator.pathFromArray([cubeName, name]);
    if (this.cubeEvaluator.byPathAnyType(pathFromArray).ownedByCube) {
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

  evaluateSymbolSql(cubeName, name, symbol, memberExpressionType) {
    const isMemberExpr = !!memberExpressionType;
    if (!memberExpressionType) {
      this.pushMemberNameForCollectionIfNecessary(cubeName, name);
    }
    const memberPathArray = [cubeName, name];
    const memberPath = this.cubeEvaluator.pathFromArray(memberPathArray);
    let type = memberExpressionType;
    if (!type && this.cubeEvaluator.isMeasure(memberPathArray)) {
      type = 'measure';
    }
    if (!type && this.cubeEvaluator.isDimension(memberPathArray)) {
      type = 'dimension';
    }
    if (!type && this.cubeEvaluator.isSegment(memberPathArray)) {
      type = 'segment';
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
        // if (symbol.postAggregate) {
        //   const orderBySql = (symbol.orderBy || []).map(o => ({ sql: this.evaluateSql(cubeName, o.sql), dir: o.dir }));
        //   const partitionBy = this.postAggregateDimensions.length ? `PARTITION BY ${this.postAggregateDimensions.map(d => d.dimensionSql()).join(', ')} ` : '';
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
        } else {
          let res = this.autoPrefixAndEvaluateSql(cubeName, symbol.sql, isMemberExpr);
          if (symbol.shiftInterval) {
            res = `(${this.addTimestampInterval(res, symbol.shiftInterval)})`;
          }
          if (this.safeEvaluateSymbolContext().convertTzForRawTimeDimension &&
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
      sqlResolveFn: options.sqlResolveFn || ((symbol, cube, n) => self.evaluateSymbolSql(cube, n, symbol)),
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
   * (measure, dimention).
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

  collectMemberNamesFor(fn) {
    const context = { memberNames: [] };
    this.evaluateSymbolSqlWithContext(
      fn,
      context
    );

    return R.uniq(context.memberNames);
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
      this.safeEvaluateSymbolContext().measuresToRender.push({ multiplied: resultMultiplied, measure: measurePath, postAggregate: symbol.postAggregate });
    }
    if (this.safeEvaluateSymbolContext().foundCompositeCubeMeasures && !parentMeasure) {
      this.safeEvaluateSymbolContext().rootMeasure.value = { multiplied: resultMultiplied, measure: measurePath, postAggregate: symbol.postAggregate };
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
    if (symbol.postAggregate) {
      const partitionBy = (this.postAggregateDimensions.length || this.postAggregateTimeDimensions.length) ?
        `PARTITION BY ${this.postAggregateDimensions.concat(this.postAggregateTimeDimensions).map(d => d.dimensionSql()).join(', ')} ` : '';
      if (symbol.type === 'rank') {
        return `${symbol.type}() OVER (${partitionBy}ORDER BY ${orderBySql.map(o => `${o.sql} ${o.dir}`).join(', ')})`;
      }
      if (!(
        R.equals(this.postAggregateDimensions.map(d => d.expressionPath()), this.dimensions.map(d => d.expressionPath())) &&
        R.equals(this.postAggregateTimeDimensions.map(d => d.expressionPath()), this.timeDimensions.map(d => d.expressionPath()))
      )) {
        let funDef;
        if (symbol.type === 'countDistinctApprox') {
          funDef = this.countDistinctApprox(evaluateSql);
        } else if (symbol.type === 'countDistinct' || symbol.type === 'count' && !symbol.sql && multiplied) {
          funDef = `count(distinct ${evaluateSql})`;
        } else if (BaseQuery.isCalculatedMeasureType(symbol.type)) {
          // TODO calculated measure type will be ungrouped
          // if (this.postAggregateDimensions.length !== this.dimensions.length) {
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
    if (BaseQuery.isCalculatedMeasureType(symbol.type)) {
      return evaluateSql;
    }
    return `${symbol.type}(${evaluateSql})`;
  }

  static isCalculatedMeasureType(type) {
    return type === 'number' || type === 'string' || type === 'time' || type === 'boolean';
  }

  /**
   TODO: support type qualifiers on min and max
  */
  static toMemberDataType(type) {
    return this.isCalculatedMeasureType(type) ? type : 'number';
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

  primaryKeySql(primaryKeyName, cubeName) {
    const primaryKeyDimension = this.cubeEvaluator.dimensionByPath([cubeName, primaryKeyName]);
    return this.evaluateSymbolSql(
      cubeName,
      primaryKeyName,
      primaryKeyDimension
    );
  }

  multipliedJoinRowResult(cubeName) {
    // this.join not initialized on collectCubeNamesForSql
    return this.join && this.join.multiplicationFactor[cubeName];
  }

  inIntegrationTimeZone(date) {
    return moment.tz(date, this.timezone);
  }

  inDbTimeZone(date) {
    return inDbTimeZone(this.timezone, this.timestampFormat(), date);
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

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  timeGroupedColumn(granularity, dimension) {
    throw new Error('Not implemented');
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
    return inflection.underscore(name).replace(/\./g, isPreAggregationName ? '_' : '__');
  }

  newSubQuery(options) {
    const QueryClass = this.constructor;
    return new QueryClass(this.compilers, this.subQueryOptions(options));
  }

  newSubQueryForCube(cube, options) {
    return this.options.queryFactory
      ? this.options.queryFactory.createQuery(cube, this.compilers, this.subQueryOptions(options))
      : this.newSubQuery(options);
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

        COALESCE: 'COALESCE({{ args_concat }})',
        CONCAT: 'CONCAT({{ args_concat }})',
        FLOOR: 'FLOOR({{ args_concat }})',
        CEIL: 'CEIL({{ args_concat }})',
        TRUNC: 'TRUNC({{ args_concat }})',
        LEAST: 'LEAST({{ args_concat }})',
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
        GREATEST: 'GREATEST({{ args_concat }})',

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
      },
      statements: {
        select: 'SELECT {% if distinct %}DISTINCT {% endif %}' +
          '{{ select_concat | map(attribute=\'aliased\') | join(\', \') }} {% if from %}\n' +
          'FROM (\n' +
          '{{ from | indent(2, true) }}\n' +
          ') AS {{ from_alias }}{% endif %}' +
          '{% if filter %}\nWHERE {{ filter }}{% endif %}' +
          '{% if group_by %}\nGROUP BY {{ group_by }}{% endif %}' +
          '{% if order_by %}\nORDER BY {{ order_by | map(attribute=\'expr\') | join(\', \') }}{% endif %}' +
          '{% if limit %}\nLIMIT {{ limit }}{% endif %}' +
          '{% if offset %}\nOFFSET {{ offset }}{% endif %}',
        group_by_exprs: '{{ group_by | map(attribute=\'index\') | join(\', \') }}',
      },
      expressions: {
        column_aliased: '{{expr}} {{quoted_alias}}',
        case: 'CASE{% if expr %} {{ expr }}{% endif %}{% for when, then in when_then %} WHEN {{ when }} THEN {{ then }}{% endfor %}{% if else_expr %} ELSE {{ else_expr }}{% endif %} END',
        is_null: '{{ expr }} IS {% if negate %}NOT {% endif %}NULL',
        binary: '({{ left }} {{ op }} {{ right }})',
        sort: '{{ expr }} {% if asc %}ASC{% else %}DESC{% endif %}{% if nulls_first %} NULLS FIRST{% endif %}',
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
        true: 'TRUE',
        false: 'FALSE',
      },
      quotes: {
        identifiers: '"',
        escape: '""'
      },
      params: {
        param: '?'
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
    };
  }

  // eslint-disable-next-line consistent-return
  preAggregationQueryForSqlEvaluation(cube, preAggregation) {
    if (preAggregation.type === 'autoRollup') {
      return this.preAggregations.autoRollupPreAggregationQuery(cube, preAggregation);
    } else if (preAggregation.type === 'rollup') {
      return this.preAggregations.rollupPreAggregationQuery(cube, preAggregation);
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

    if (refreshKey.timezone) {
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
      return [this.floorSql(`(${this.unixTimestampSql()}) / ${this.parseSecondDuration(every)}`), external, this];
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
    const intervalMatch = interval.match(/^(\d+) (second|minute|hour|day|week|month|quarter|year)s?$/);
    if (!intervalMatch) {
      throw new UserError(`Invalid interval: ${interval}`);
    }

    const duration = parseInt(intervalMatch[1], 10);
    if (duration < 1) {
      throw new UserError(`Duration should be positive: ${interval}`);
    }

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

  preAggregationInvalidateKeyQueries(cube, preAggregation, preAggregationName) {
    return this.cacheValue(
      ['preAggregationInvalidateKeyQueries', cube, JSON.stringify(preAggregation)],
      () => {
        const preAggregationQueryForSql = this.preAggregationQueryForSqlEvaluation(cube, preAggregation);
        if (preAggregation.refreshKey) {
          if (preAggregation.refreshKey.sql) {
            return [
              this.paramAllocator.buildSqlAndParams(
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
      securityContext: BaseQuery.contextSymbolsProxyFrom({}, allocateParam),
    };
  }

  contextSymbolsProxy(symbols) {
    return BaseQuery.contextSymbolsProxyFrom(symbols, this.paramAllocator.allocateParam.bind(this.paramAllocator));
  }

  static contextSymbolsProxyFrom(symbols, allocateParam) {
    return new Proxy(symbols, {
      get: (target, name) => {
        const propValue = target[name];
        const methods = (paramValue) => ({
          filter: (column) => {
            if (paramValue) {
              const value = Array.isArray(paramValue) ?
                paramValue.map(allocateParam) :
                allocateParam(paramValue);
              if (typeof column === 'function') {
                return column(value);
              } else {
                return `${column} = ${value}`;
              }
            } else {
              return '1 = 1';
            }
          },
          requiredFilter: (column) => {
            if (!paramValue) {
              throw new UserError(`Filter for ${column} is required`);
            }
            return methods(paramValue).filter(column);
          },
          unsafeValue: () => paramValue
        });
        return methods(target)[name] ||
          typeof propValue === 'object' && propValue !== null && BaseQuery.contextSymbolsProxyFrom(propValue, allocateParam) ||
          methods(propValue);
      }
    });
  }

  static extractFilterMembers(filter) {
    if (filter.operator === 'and' || filter.operator === 'or') {
      return filter.values.map(f => BaseQuery.extractFilterMembers(f)).reduce((a, b) => ((a && b) ? { ...a, ...b } : null), {});
    } else if (filter.measure || filter.dimension) {
      return {
        [filter.measure || filter.dimension]: true
      };
    } else {
      return null;
    }
  }

  static findAndSubTreeForFilterGroup(filter, groupMembers, newGroupFilter) {
    if ((filter.operator === 'and' || filter.operator === 'or') && !filter.values?.length) {
      return null;
    }
    const filterMembers = BaseQuery.extractFilterMembers(filter);
    if (filterMembers && Object.keys(filterMembers).every(m => groupMembers.indexOf(m) !== -1)) {
      return filter;
    }
    if (filter.operator === 'and') {
      const result = filter.values.map(f => BaseQuery.findAndSubTreeForFilterGroup(f, groupMembers, newGroupFilter)).filter(f => !!f);
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

  static renderFilterParams(filter, filterParamArgs, allocateParam, newGroupFilter) {
    if (!filter) {
      return '1 = 1';
    }

    if (filter.operator === 'and' || filter.operator === 'or') {
      const values = filter.values
        .map(f => BaseQuery.renderFilterParams(f, filterParamArgs, allocateParam, newGroupFilter))
        .map(v => ({ filterToWhere: () => v }));

      return newGroupFilter({ operator: filter.operator, values }).filterToWhere();
    }

    const filterParams = filter && filter.filterParams();
    const filterParamArg = filterParamArgs.filter(p => p.__member() === filter.measure || p.__member() === filter.dimension)[0];
    if (!filterParamArg) {
      throw new Error(`FILTER_PARAMS arg not found for ${filter.measure || filter.dimension}`);
    }
    if (
      filterParams && filterParams.length
    ) {
      if (typeof filterParamArg.__column() === 'function') {
        // eslint-disable-next-line prefer-spread
        return filterParamArg.__column().apply(
          null,
          filterParams.map(allocateParam),
        );
      } else {
        return filter.conditionSql(filterParamArg.__column());
      }
    } else {
      return '1 = 1';
    }
  }

  filterGroupFunction() {
    const { allFilters } = this;
    const allocateParam = this.paramAllocator.allocateParam.bind(this.paramAllocator);
    const newGroupFilter = this.newGroupFilter.bind(this);
    return (...filterParamArgs) => {
      const groupMembers = filterParamArgs.map(f => {
        if (!f.__member) {
          throw new UserError(`FILTER_GROUP expects FILTER_PARAMS args to be passed. For example FILTER_GROUP(FILTER_PARAMS.foo.bar.filter('bar'), FILTER_PARAMS.foo.jar.filter('jar')). But found: ${f}`);
        }
        return f.__member();
      });

      const filter = BaseQuery.findAndSubTreeForFilterGroup(newGroupFilter({ operator: 'and', values: allFilters }), groupMembers, newGroupFilter);

      return `(${BaseQuery.renderFilterParams(filter, filterParamArgs, allocateParam, newGroupFilter)})`;
    };
  }

  static filterProxyFromAllFilters(allFilters, cubeEvaluator, allocateParam, newGroupFilter) {
    return new Proxy({}, {
      get: (target, name) => {
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
                const filter = BaseQuery.findAndSubTreeForFilterGroup(
                  newGroupFilter({ operator: 'and', values: allFilters }),
                  [cubeEvaluator.pathFromArray([cubeNameObj.cube, propertyName])],
                  newGroupFilter
                );
                return `(${BaseQuery.renderFilterParams(filter, [this], allocateParam, newGroupFilter)})`;
              }
            })
          })
        });
      }
    });
  }
}
