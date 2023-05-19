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
import {
  FROM_PARTITION_RANGE,
  MAX_SOURCE_ROW_LIMIT,
  inDbTimeZone,
  QueryAlias,
  getEnv,
} from '@cubejs-backend/shared';

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
 * @property {DataSchemaCompiler} compiler
 * @property {CubeToMetaTransformer} metaTransformer
 * @property {CubeEvaluator} cubeEvaluator
 * @property {ContextEvaluator} contextEvaluator
 * @property {JoinGraph} joinGraph
 * @property {CompilerCache} compilerCache
 * @property {*} headCommitId
 */

export

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
class BaseQuery {
  /**
   * BaseQuery class constructor.
   * @param {Compilers|*} compilers
   * @param {*} options
   */
  constructor(compilers, options) {
    this.compilers = compilers;
    this.cubeEvaluator = compilers.cubeEvaluator;
    this.joinGraph = compilers.joinGraph;
    this.options = options || {};

    this.orderHashToString = this.orderHashToString.bind(this);
    this.defaultOrder = this.defaultOrder.bind(this);

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
            dimension: dimension[0],
            measure: null,
          };
        }
        if (!dimension.length && measure.length) {
          return {
            values: this.extractFiltersAsTree(f[operator]),
            operator,
            dimension: null,
            measure: measure[0],
          };
        }
        if (!dimension.length && !measure.length) {
          return {
            values: [],
            operator,
            dimension: null,
            measure: null,
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

  initFromOptions() {
    this.contextSymbols = {
      securityContext: {},
      ...this.options.contextSymbols,
    };
    /**
     * @protected
     * @type {ParamAllocator}
     */
    this.paramAllocator = this.options.paramAllocator || this.newParamAllocator();
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
      useOriginalSqlPreAggregationsInPreAggregation: this.options.useOriginalSqlPreAggregationsInPreAggregation,
      cubeLatticeCache: this.options.cubeLatticeCache, // TODO too heavy for key
      historyQueries: this.options.historyQueries, // TODO too heavy for key
      ungrouped: this.options.ungrouped
    });
    this.timezone = this.options.timezone;
    this.rowLimit = this.options.rowLimit;
    this.offset = this.options.offset;
    this.preAggregations = this.newPreAggregations();
    this.measures = (this.options.measures || []).map(this.newMeasure.bind(this));
    this.dimensions = (this.options.dimensions || []).map(this.newDimension.bind(this));
    this.segments = (this.options.segments || []).map(this.newSegment.bind(this));
    this.order = this.options.order || [];
    const filters = this.extractFiltersAsTree(this.options.filters || []);

    // measure_filter (the one extracted from filters parameter on measure and
    // used in drill downs) should go to WHERE instead of HAVING
    this.filters = filters.filter(f => f.dimension || f.operator === 'measure_filter' || f.operator === 'measureFilter').map(this.initFilter.bind(this));
    this.measureFilters = filters.filter(f => f.measure && f.operator !== 'measure_filter' && f.operator !== 'measureFilter').map(this.initFilter.bind(this));

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

  initFilter(filter) {
    if (filter.operator === 'and' || filter.operator === 'or') {
      filter.values = filter.values.map(this.initFilter.bind(this));
      return this.newGroupFilter(filter);
    }
    return this.newFilter(filter);
  }

  newFilter(filter) {
    return new BaseFilter(this, filter);
  }

  newGroupFilter(filter) {
    return new BaseGroupFilter(this, filter);
  }

  newTimeDimension(timeDimension) {
    return new BaseTimeDimension(this, timeDimension);
  }

  newParamAllocator() {
    return new ParamAllocator();
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
    if (!this.options.preAggregationQuery && !this.ungrouped) {
      preAggForQuery =
        this.preAggregations.findPreAggregationForQuery();
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
    if (!this.options.preAggregationQuery && this.externalQueryClass) {
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
   * @returns {Array<string>}
   */
  buildSqlAndParams() {
    if (!this.options.preAggregationQuery && this.externalQueryClass) {
      if (this.externalPreAggregationQuery()) { // TODO performance
        return this.externalQuery().buildSqlAndParams();
      }
    }

    return this.compilers.compiler.withQuery(
      this,
      () => this.cacheValue(
        ['buildSqlAndParams'],
        () => this.paramAllocator.buildSqlAndParams(
          this.buildParamAnnotatedSql()
        ),
        { cache: this.queryCache }
      )
    );
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
    const {
      multipliedMeasures,
      regularMeasures,
      cumulativeMeasures,
    } = this.fullKeyQueryAggregateMeasures();

    if (!multipliedMeasures.length && !cumulativeMeasures.length) {
      return this.simpleQuery();
    }

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
          );
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
    return this.joinFullKeyQueryAggregate(
      multipliedMeasures,
      regularMeasures,
      cumulativeMeasures,
      toJoin,
    );
  }

  joinFullKeyQueryAggregate(
    multipliedMeasures,
    regularMeasures,
    cumulativeMeasures,
    toJoin,
  ) {
    const renderedReferenceContext = {
      renderedReference: R.pipe(
        R.map(m => [m.measure, m.aliasName()]),
        R.fromPairs,
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      )(multipliedMeasures.concat(regularMeasures).concat(cumulativeMeasures.map(([multiplied, measure]) => measure))),
    };

    const join = R.drop(1, toJoin)
      .map(
        (q, i) => (this.dimensionAliasNames().length ?
          `INNER JOIN (${q}) as q_${i + 1} ON ${this.dimensionsJoinCondition(`q_${i}`, `q_${i + 1}`)}` :
          `, (${q}) as q_${i + 1}`),
      ).join('\n');

    const columnsToSelect = this.evaluateSymbolSqlWithContext(
      () => this.dimensionColumns('q_0').concat(this.measures.map(m => m.selectColumns())).join(', '),
      renderedReferenceContext,
    );

    const queryHasNoRemapping = this.evaluateSymbolSqlWithContext(
      () => this.dimensionsForSelect().concat(this.measures).every(r => r.hasNoRemapping()),
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
      this.measures.filter(m => m.expression).length === 0 &&
      queryHasNoRemapping
    ) {
      return `${toJoin[0].replace(/^SELECT/, `SELECT ${this.topLimit()}`)} ${this.orderBy()}${this.groupByDimensionLimit()}`;
    }

    return `SELECT ${this.topLimit()}${columnsToSelect} FROM (${toJoin[0]}) as q_0 ${join}${havingFilters}${this.orderBy()}${this.groupByDimensionLimit()}`;
  }

  fullKeyQueryAggregateMeasures() {
    const measureToHierarchy = this.collectRootMeasureToHieararchy();

    const measuresToRender = (multiplied, cumulative) => R.pipe(
      R.values,
      R.flatten,
      R.filter(
        m => m.multiplied === multiplied && this.newMeasure(m.measure).isCumulative() === cumulative
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
    return { multipliedMeasures, regularMeasures, cumulativeMeasures };
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

  dateSeriesSql(timeDimension) {
    return `(${this.seriesSql(timeDimension)}) ${this.asSyntaxTable} ${timeDimension.dateSeriesAliasName()}`;
  }

  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `('${from}', '${to}')`
    );
    return `SELECT ${this.dateTimeCast('date_from')} as ${this.escapeColumnName('date_from')}, ${this.dateTimeCast('date_to')} as ${this.escapeColumnName('date_to')} FROM (VALUES ${values}) ${this.asSyntaxTable} dates (date_from, date_to)`;
  }

  /**
   * @param {string} timeDimension
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

  afterDateFilter(dimensionSql, timeStampParam) {
    return `${dimensionSql} > ${timeStampParam}`;
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

  collectRootMeasureToHieararchy() {
    const notAddedMeasureFilters = R.flatten(this.measureFilters.map(f => f.getMembers()))
      .filter(f => R.none(m => m.measure === f.measure, this.measures));

    return R.fromPairs(this.measures.concat(notAddedMeasureFilters).map(m => {
      const collectedMeasures = this.collectFrom(
        [m],
        this.collectMultipliedMeasures.bind(this),
        'collectMultipliedMeasures',
        this.queryCache
      );
      if (m.expressionName && !collectedMeasures.length) {
        throw new UserError(`Subquery dimension ${m.expressionName} should reference at least one measure`);
      }
      return [m.measure, collectedMeasures];
    }));
  }

  query() {
    return this.joinQuery(this.join, this.collectFromMembers(
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
    return {
      sql: `(${sql})`,
      alias: subQueryAlias,
      on: primaryKeys.map((pk) => `${subQueryAlias}.${this.newDimension(this.primaryKeyName(cubeName, pk)).aliasName()} = ${this.primaryKeySql(pk, cubeName)}`)
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
    const membersToCollectFrom = this.measures
      .concat(this.dimensions)
      .concat(this.segments)
      .concat(this.filters)
      .concat(this.measureFilters)
      .concat(excludeTimeDimensions ? [] : this.timeDimensions)
      .concat(this.join ? this.join.joins.map(j => ({
        getMembers: () => [{
          path: () => null,
          cube: () => this.cubeEvaluator.cubeFromPath(j.originalFrom),
          definition: () => j.join,
        }]
      })) : []);
    return this.collectFrom(membersToCollectFrom, fn, methodName);
  }

  collectFrom(membersToCollectFrom, fn, methodName, cache) {
    return R.pipe(
      R.map(f => f.getMembers()),
      R.flatten,
      R.map(s => (
        (cache || this.compilerCache).cache(
          ['collectFrom', methodName].concat(
            s.path() ? [s.path().join('.')] : [s.cube().name, s.expressionName || s.definition().sql]
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
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.map((c, i) => `${i + 1}`).join(', ')}` : '';
  }

  getFieldIndex(id) {
    const equalIgnoreCase = (a, b) => (
      typeof a === 'string' && typeof b === 'string' && a.toUpperCase() === b.toUpperCase()
    );

    let index;

    index = this.dimensionsForSelect().findIndex(
      d => equalIgnoreCase(d.dimension, id)
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
    let limitClause = '';
    if (this.rowLimit !== null) {
      if (this.rowLimit === MAX_SOURCE_ROW_LIMIT) {
        limitClause = ` LIMIT ${this.paramAllocator.allocateParam(MAX_SOURCE_ROW_LIMIT)}`;
      } else if (typeof this.rowLimit === 'number') {
        limitClause = ` LIMIT ${this.rowLimit}`;
      }
    }
    const offsetClause = this.offset ? ` OFFSET ${parseInt(this.offset, 10)}` : '';
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

  forSelect() {
    return this.dimensionsForSelect().concat(this.measures);
  }

  /**
   * Returns a complete list of the dimensions, including time dimensions.
   * @returns {Array<BaseDimension>}
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

  autoPrefixWithCubeName(cubeName, sql) {
    if (sql.match(/^[_a-zA-Z][_a-zA-Z0-9]*$/)) {
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

  evaluateSymbolSql(cubeName, name, symbol) {
    this.pushMemberNameForCollectionIfNecessary(cubeName, name);
    const memberPathArray = [cubeName, name];
    const memberPath = this.cubeEvaluator.pathFromArray(memberPathArray);
    if (this.cubeEvaluator.isMeasure(memberPathArray)) {
      let parentMeasure;
      if (this.safeEvaluateSymbolContext().compositeCubeMeasures ||
        this.safeEvaluateSymbolContext().leafMeasures) {
        parentMeasure = this.safeEvaluateSymbolContext().currentMeasure;
        if (this.safeEvaluateSymbolContext().compositeCubeMeasures) {
          if (parentMeasure &&
            (
              this.cubeEvaluator.cubeNameFromPath(parentMeasure) !== cubeName ||
              this.newMeasure(this.cubeEvaluator.pathFromArray(memberPathArray)).isCumulative()
            )
          ) {
            this.safeEvaluateSymbolContext().compositeCubeMeasures[parentMeasure] = true;
          }
        }
        this.safeEvaluateSymbolContext().currentMeasure = this.cubeEvaluator.pathFromArray(memberPathArray);
        if (this.safeEvaluateSymbolContext().leafMeasures) {
          if (parentMeasure) {
            this.safeEvaluateSymbolContext().leafMeasures[parentMeasure] = false;
          }
          this.safeEvaluateSymbolContext().leafMeasures[this.safeEvaluateSymbolContext().currentMeasure] = true;
        }
      }
      const primaryKeys = this.cubeEvaluator.primaryKeys[cubeName];
      const result = this.renderSqlMeasure(
        name,
        this.applyMeasureFilters(
          this.autoPrefixWithCubeName(
            cubeName,
            symbol.sql && this.evaluateSql(cubeName, symbol.sql) ||
            primaryKeys.length && (
              primaryKeys.length > 1 ?
                this.concatStringsSql(primaryKeys.map((pk) => this.castToString(this.primaryKeySql(pk, cubeName))))
                : this.primaryKeySql(primaryKeys[0], cubeName)
            ) || '*'
          ),
          symbol,
          cubeName
        ),
        symbol,
        cubeName,
        parentMeasure
      );
      if (
        this.safeEvaluateSymbolContext().compositeCubeMeasures ||
        this.safeEvaluateSymbolContext().leafMeasures
      ) {
        this.safeEvaluateSymbolContext().currentMeasure = parentMeasure;
      }
      return result;
    } else if (this.cubeEvaluator.isDimension(memberPathArray)) {
      if ((this.safeEvaluateSymbolContext().renderedReference || {})[memberPath]) {
        return this.evaluateSymbolContext.renderedReference[memberPath];
      }
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
          this.autoPrefixAndEvaluateSql(cubeName, symbol.latitude.sql),
          '\',\'',
          this.autoPrefixAndEvaluateSql(cubeName, symbol.longitude.sql)
        ]);
      } else {
        return this.autoPrefixAndEvaluateSql(cubeName, symbol.sql);
      }
    } else if (this.cubeEvaluator.isSegment(memberPathArray)) {
      if ((this.safeEvaluateSymbolContext().renderedReference || {})[memberPath]) {
        return this.evaluateSymbolContext.renderedReference[memberPath];
      }
      return this.autoPrefixWithCubeName(cubeName, this.evaluateSql(cubeName, symbol.sql));
    }
    return this.evaluateSql(cubeName, symbol.sql);
  }

  autoPrefixAndEvaluateSql(cubeName, sql) {
    return this.autoPrefixWithCubeName(cubeName, this.evaluateSql(cubeName, sql));
  }

  concatStringsSql(strings) {
    return strings.join(' || ');
  }

  primaryKeyNames(cubeName) {
    const primaryKeys = this.cubeEvaluator.primaryKeys[cubeName];
    if (!primaryKeys || !primaryKeys.length) {
      throw new UserError(`One or more Primary key is required for '${cubeName}`);
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

  collectMultipliedMeasures(fn) {
    const foundCompositeCubeMeasures = {};
    this.evaluateSymbolSqlWithContext(
      fn,
      { compositeCubeMeasures: foundCompositeCubeMeasures }
    );

    const renderContext = {
      measuresToRender: [], foundCompositeCubeMeasures, compositeCubeMeasures: {}, rootMeasure: {}
    };
    this.evaluateSymbolSqlWithContext(
      fn,
      renderContext
    );
    return renderContext.measuresToRender.length ?
      R.uniq(renderContext.measuresToRender) :
      [renderContext.rootMeasure.value];
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

  renderSqlMeasure(name, evaluateSql, symbol, cubeName, parentMeasure) {
    const multiplied = this.multipliedJoinRowResult(cubeName);
    const measurePath = `${cubeName}.${name}`;
    let resultMultiplied = multiplied;
    if (multiplied && (
      symbol.type === 'number' && evaluateSql === 'count(*)' ||
      symbol.type === 'countDistinct' ||
      symbol.type === 'count' && !symbol.sql)
    ) {
      resultMultiplied = false;
    }
    if (parentMeasure &&
      (this.safeEvaluateSymbolContext().foundCompositeCubeMeasures || {})[parentMeasure] &&
      !(this.safeEvaluateSymbolContext().foundCompositeCubeMeasures || {})[measurePath]
    ) {
      this.safeEvaluateSymbolContext().measuresToRender.push({ multiplied: resultMultiplied, measure: measurePath });
    }
    if (this.safeEvaluateSymbolContext().foundCompositeCubeMeasures && !parentMeasure) {
      this.safeEvaluateSymbolContext().rootMeasure.value = { multiplied: resultMultiplied, measure: measurePath };
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
        return '1';
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
  aliasName(name, isPreAggregationName) {
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

  sqlTemplates() {
    return {
      functions: {
        sum: 'SUM({{ argument }})',
      }
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
   * @return {(number|*)[]}
   */
  parseInterval(interval) {
    const intervalMatch = interval.match(/^(\d+) (second|minute|hour|day|week)s?$/);
    if (!intervalMatch) {
      throw new UserError(`Invalid interval: ${interval}`);
    }

    const duration = parseInt(intervalMatch[1], 10);
    if (duration < 1) {
      throw new UserError(`Duration should be positive: ${interval}`);
    }

    return [duration, intervalMatch[2]];
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

  preAggregationInvalidateKeyQueries(cube, preAggregation) {
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
              throw new UserError('Incremental refresh key can only be used for partitioned pre-aggregations');
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
      filterParams: BaseQuery.filterProxyFromAllFilters(null, cubeEvaluator, allocateParam),
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

  filtersProxy() {
    const { allFilters } = this;
    return BaseQuery.filterProxyFromAllFilters(
      allFilters,
      this.cubeEvaluator,
      this.paramAllocator.allocateParam.bind(this.paramAllocator)
    );
  }

  static filterProxyFromAllFilters(allFilters, cubeEvaluator, allocateParam) {
    return new Proxy({}, {
      get: (target, name) => {
        if (name === '_objectWithResolvedProperties') {
          return true;
        }
        // allFilters is null whenever it's used to test if the member is owned by cube so it should always render to `1 = 1`
        // and do not check cube validity as it's part of compilation step.
        const cubeName = allFilters && cubeEvaluator.cubeNameFromPath(name);
        return new Proxy({ cube: cubeName }, {
          get: (cubeNameObj, propertyName) => {
            const filters =
              allFilters?.filter(f => f.dimension === cubeEvaluator.pathFromArray([cubeNameObj.cube, propertyName]));
            return {
              filter: (column) => {
                if (!filters || !filters.length) {
                  return '1 = 1';
                }

                return filters.map(filter => {
                  const filterParams = filter && filter.filterParams();
                  if (
                    filterParams && filterParams.length
                  ) {
                    if (typeof column === 'function') {
                      // eslint-disable-next-line prefer-spread
                      return column.apply(
                        null,
                        filterParams.map(allocateParam),
                      );
                    } else {
                      return filter.conditionSql(column);
                    }
                  } else {
                    return '1 = 1';
                  }
                }).join(' AND ');
              }
            };
          }
        });
      }
    });
  }
}
