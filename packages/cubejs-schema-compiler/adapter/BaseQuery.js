/* eslint-disable no-unused-vars,prefer-template */
const R = require('ramda');
const moment = require('moment-timezone');
const inflection = require('inflection');

const UserError = require('../compiler/UserError');
const BaseMeasure = require('./BaseMeasure');
const BaseDimension = require('./BaseDimension');
const BaseSegment = require('./BaseSegment');
const BaseFilter = require('./BaseFilter');
const BaseTimeDimension = require('./BaseTimeDimension');
const ParamAllocator = require('./ParamAllocator');
const PreAggregations = require('./PreAggregations');
const SqlParser = require('../parser/SqlParser');

const DEFAULT_PREAGGREGATIONS_SCHEMA = `stb_pre_aggregations`;

const standardGranularitiesParents = {
  year: 'month',
  week: 'day',
  month: 'day',
  day: 'hour',
  hour: 'minute',
  minute: 'second'
};

const SecondsDurations = {
  week: 60 * 60 * 24 * 7,
  day: 60 * 60 * 24,
  hour: 60 * 60,
  minute: 60,
  second: 1
};

class BaseQuery {
  constructor(compilers, options) {
    this.compilers = compilers;
    this.cubeEvaluator = compilers.cubeEvaluator;
    this.joinGraph = compilers.joinGraph;
    this.options = options || {};

    this.orderHashToString = this.orderHashToString.bind(this);
    this.defaultOrder = this.defaultOrder.bind(this);

    this.initFromOptions();

    this.granularityParentHierarchyCache = {};
  }

  initFromOptions() {
    this.contextSymbols = Object.assign({ userContext: {} }, this.options.contextSymbols || {});
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
    const filters = (this.options.filters || []).map(f => {
      if (this.cubeEvaluator.isMeasure(f.dimension)) {
        return Object.assign({}, f, {
          dimension: null,
          measure: f.dimension
        });
      }
      return f;
    });

    // measure_filter (the one extracted from filters parameter on measure and
    // used in drill downs) should go to WHERE instead of HAVING
    this.filters = filters.filter(f => f.dimension || f.operator === 'measure_filter').map(this.newFilter.bind(this));
    this.measureFilters = filters.filter(f => f.measure && f.operator !== 'measure_filter').map(this.newFilter.bind(this));

    this.timeDimensions = (this.options.timeDimensions || []).map(dimension => {
      if (!dimension.dimension) {
        const join = this.joinGraph.buildJoin(this.collectCubeNames(true));
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
    this.join = this.joinGraph.buildJoin(this.allCubeNames);
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

  get dataSource() {
    const dataSources = R.uniq(this.allCubeNames.map(c => this.cubeDataSource(c)));
    if (dataSources.length > 1) {
      throw new UserError(`Joins across data sources aren't supported in community edition. Found data sources: ${dataSources.join(', ')}`);
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
        const primaryKeyNames = cubes.map(c => this.primaryKeyName(c));
        const missingPrimaryKeys = primaryKeyNames.filter(key => !this.dimensions.find(d => d.dimension === key));
        if (missingPrimaryKeys.length) {
          throw new UserError(`Ungrouped query requires primary keys to be present in dimensions: ${missingPrimaryKeys.map(k => `'${k}'`).join(', ')}. Pass allowUngroupedWithoutPrimaryKey option to disable this check.`);
        }
      }
      if (this.measures.length) {
        throw new UserError(`Measures aren't allowed in ungrouped query`);
      }
      if (this.measureFilters.length) {
        throw new UserError(`Measure filters aren't allowed in ungrouped query`);
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

  newFilter(filter) {
    return new BaseFilter(this, filter);
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

  escapeColumnName(name) {
    return `"${name}"`;
  }

  buildParamAnnotatedSql() {
    if (!this.options.preAggregationQuery && !this.ungrouped) {
      const preAggregationForQuery = this.preAggregations.findPreAggregationForQuery();
      if (preAggregationForQuery) {
        return this.preAggregations.rollupPreAggregation(preAggregationForQuery);
      }
    }
    return this.fullKeyQueryAggregate();
  }

  externalPreAggregationQuery() {
    if (!this.options.preAggregationQuery && this.externalQueryClass) {
      const preAggregationForQuery = this.preAggregations.findPreAggregationForQuery();
      if (preAggregationForQuery && preAggregationForQuery.preAggregation.external) {
        return true;
      }
      const preAggregationsDescription = this.preAggregations.preAggregationsDescription();
      return preAggregationsDescription.length && R.all((p) => p.external, preAggregationsDescription);
    }
    return false;
  }

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
        () => this.paramAllocator.buildSqlAndParams(this.buildParamAnnotatedSql()),
        { cache: this.queryCache }
      )
    );
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

  fullKeyQueryAggregate() {
    const { multipliedMeasures, regularMeasures, cumulativeMeasures } = this.fullKeyQueryAggregateMeasures();

    if (!multipliedMeasures.length && !cumulativeMeasures.length) {
      return this.simpleQuery();
    }

    const renderedReferenceContext = {
      renderedReference: R.pipe(
        R.map(m => [m.measure, m.aliasName()]),
        R.fromPairs
      )(multipliedMeasures.concat(regularMeasures).concat(cumulativeMeasures.map(([multiplied, measure]) => measure)))
    };

    const toJoin =
      (regularMeasures.length ? [
        this.withCubeAliasPrefix('main', () => this.regularMeasuresSubQuery(regularMeasures))
      ] : [])
        .concat(
          R.pipe(
            R.groupBy(m => m.cube().name),
            R.toPairs,
            R.map(
              ([keyCubeName, measures]) => this.withCubeAliasPrefix(`${keyCubeName}_key`, () => this.aggregateSubQuery(keyCubeName, measures))
            )
          )(multipliedMeasures)
        ).concat(
          R.map(
            ([multiplied, measure]) => this.withCubeAliasPrefix(
              `${this.aliasName(measure.measure.replace('.', '_'))}_cumulative`,
              () => this.overTimeSeriesQuery(
                multiplied ?
                  (measures, filters) => this.aggregateSubQuery(measures[0].cube().name, measures, filters) :
                  this.regularMeasuresSubQuery.bind(this),
                measure
              )
            )
          )(cumulativeMeasures)
        );

    const join = R.drop(1, toJoin)
      .map(
        (q, i) => (this.dimensionAliasNames().length ?
          `INNER JOIN (${q}) as q_${i + 1} ON ${this.dimensionsJoinCondition(`q_${i}`, `q_${i + 1}`)}` :
          `, (${q}) as q_${i + 1}`)
      ).join("\n");

    const columnsToSelect = this.evaluateSymbolSqlWithContext(
      () => this.dimensionColumns('q_0').concat(this.measures.map(m => m.selectColumns())).join(', '),
      renderedReferenceContext
    );
    const havingFilters = this.evaluateSymbolSqlWithContext(
      () => this.baseWhere(this.measureFilters),
      renderedReferenceContext
    );
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

  granularityParent(granularity) {
    return standardGranularitiesParents[granularity];
  }

  granularityParentHierarchy(granularity) {
    if (!this.granularityParentHierarchyCache[granularity]) {
      this.granularityParentHierarchyCache[granularity] = [granularity].concat(
        this.granularityParent(granularity) ? this.granularityParentHierarchy(this.granularityParent(granularity)) : []
      );
    }
    return this.granularityParentHierarchyCache[granularity];
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

  overTimeSeriesQuery(baseQueryFn, cumulativeMeasure) {
    const dateJoinCondition = cumulativeMeasure.dateJoinCondition();
    const cumulativeMeasures = [cumulativeMeasure];
    const dateFromStartToEndConditionSql =
      (isFromStartToEnd) => dateJoinCondition.map(
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
              `${d.convertedToTz()}`,
              `${this.timeStampInClientTz(d.dateFromParam())}`,
              `${this.timeStampInClientTz(d.dateToParam())}`,
              isFromStartToEnd
            );
          }
        })
      );
    if (!this.timeDimensions.find(d => d.granularity)) {
      const filters = this.segments.concat(this.filters).concat(dateFromStartToEndConditionSql(false));
      return baseQueryFn(cumulativeMeasures, filters, false);
    }
    const dateSeriesSql = this.timeDimensions.map(d => this.dateSeriesSql(d)).join(', ');
    const filters = this.segments.concat(this.filters).concat(dateFromStartToEndConditionSql(true));
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

  overTimeSeriesSelect(cumulativeMeasures, dateSeriesSql, baseQuery, dateJoinConditionSql, baseQueryAlias) {
    const forSelect = this.dateSeriesSelect().concat(
      this.dimensions.concat(cumulativeMeasures).map(s => s.cumulativeSelectColumns())
    ).filter(c => !!c).join(', ');
    return `SELECT ${forSelect} FROM ${dateSeriesSql}` +
      ` LEFT JOIN (${baseQuery}) ${this.asSyntaxJoin} ${baseQueryAlias} ON ${dateJoinConditionSql}` +
      this.groupByClause();
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
    return `SELECT ${this.dateTimeCast('date_from')}, ${this.dateTimeCast('date_to')} FROM (VALUES ${values}) ${this.asSyntaxTable} dates (date_from, date_to)`;
  }

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
    const notAddedMeasureFilters = this.measureFilters.filter(f => R.none(m => m.measure === f.measure, this.measures));
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
    // TODO params independent sql caching
    const parser = this.queryCache.cache(['SqlParser', sql], () => new SqlParser(sql));
    if (
      this.cubeEvaluator.cubeFromPath(cube).rewriteQueries &&
      parser.isSimpleAsteriskQuery()
    ) {
      const conditions = parser.extractWhereConditions(cubeAlias);
      if (!isLeftJoinCondition && this.safeEvaluateSymbolContext().inlineWhereConditions) {
        this.safeEvaluateSymbolContext().inlineWhereConditions.push({ filterToWhere: () => conditions });
      }
      return [parser.extractTableFrom(), cubeAlias, conditions];
    } else {
      return [sql, cubeAlias];
    }
  }

  joinQuery(join, subQueryDimensions) {
    const joins = join.joins.map(
      j => {
        const [cubeSql, cubeAlias, conditions] = this.rewriteInlineCubeSql(j.originalTo, true);
        return `LEFT JOIN ${cubeSql} ${this.asSyntaxJoin} ${cubeAlias}
      ON ${this.evaluateSql(j.originalFrom, j.join.sql)}${conditions ? ` AND (${conditions})` : ''}`;
      }
    ).concat(subQueryDimensions.map(d => this.subQueryJoin(d)));

    const [cubeSql, cubeAlias] = this.rewriteInlineCubeSql(join.root);
    return `${cubeSql} ${this.asSyntaxJoin} ${cubeAlias}\n${joins.join("\n")}`;
  }

  subQueryJoin(dimension) {
    const { prefix, subQuery, cubeName } = this.subQueryDescription(dimension);
    const primaryKey = this.newDimension(this.primaryKeyName(cubeName));
    const subQueryAlias = this.escapeColumnName(this.aliasName(prefix));

    const { collectOriginalSqlPreAggregations } = this.safeEvaluateSymbolContext();
    const sql = subQuery.evaluateSymbolSqlWithContext(() => subQuery.buildParamAnnotatedSql(), {
      collectOriginalSqlPreAggregations
    });
    return `LEFT JOIN (${sql}) ${this.asSyntaxJoin} ${subQueryAlias}
    ON ${subQueryAlias}.${primaryKey.aliasName()} = ${this.primaryKeySql(this.cubeEvaluator.primaryKeys[cubeName], cubeName)}`;
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
      dimensions: [this.primaryKeyName(cubeName)],
      filters,
      segments,
      timeDimensions
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

  aggregateSubQuery(keyCubeName, measures, filters) {
    filters = filters || this.allFilters;
    const primaryKeyDimension = this.newDimension(this.primaryKeyName(keyCubeName));
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
      const cubes = this.collectFrom(measures, this.collectCubeNamesFor.bind(this), 'collectCubeNamesFor');
      const measuresJoin = this.joinGraph.buildJoin(cubes);
      if (measuresJoin.multiplicationFactor[keyCubeName]) {
        throw new UserError(
          `'${measures.map(m => m.measure).join(', ')}' reference cubes that lead to row multiplication.`
        );
      }
      keyCubeSql = `(${this.aggregateSubQueryMeasureJoin(keyCubeName, measures, measuresJoin, primaryKeyDimension, measureSubQueryDimensions)})`;
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
    const columnsForSelect =
      this.dimensionColumns(this.escapeColumnName('keys')).concat(selectedMeasures).filter(s => !!s).join(', ');
    const keyInMeasureSelect = shouldBuildJoinForMeasureSelect ?
      `${this.cubeAlias(keyCubeName)}.${primaryKeyDimension.aliasName()}` :
      this.dimensionSql(primaryKeyDimension);
    const subQueryJoins =
      shouldBuildJoinForMeasureSelect ? '' : measureSubQueryDimensions.map(d => this.subQueryJoin(d)).join("\n");
    return `SELECT ${columnsForSelect} FROM (${this.keysQuery(primaryKeyDimension, filters)}) ${this.asSyntaxTable} ${this.escapeColumnName('keys')} ` +
      `LEFT OUTER JOIN ${keyCubeSql} ${this.asSyntaxJoin} ${keyCubeAlias} ON
      ${this.escapeColumnName('keys')}.${primaryKeyDimension.aliasName()} = ${keyInMeasureSelect}
      ${keyCubeInlineLeftJoinConditions ? ` AND (${keyCubeInlineLeftJoinConditions})` : ''}` +
      subQueryJoins +
      (!this.safeEvaluateSymbolContext().ungrouped && this.groupByClause() || '');
  }

  checkShouldBuildJoinForMeasureSelect(measures, keyCubeName) {
    return measures.map(measure => {
      const cubeNames = this.collectFrom([measure], this.collectCubeNamesFor.bind(this), 'collectCubeNamesFor');
      if (R.any(cubeName => keyCubeName !== cubeName, cubeNames)) {
        const measuresJoin = this.joinGraph.buildJoin(cubeNames);
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

  aggregateSubQueryMeasureJoin(keyCubeName, measures, measuresJoin, primaryKeyDimension, measureSubQueryDimensions) {
    return this.ungroupedMeasureSelect(() => this.withCubeAliasPrefix(`${keyCubeName}_measure_join`,
      () => {
        const columns = [primaryKeyDimension.selectColumns()].concat(measures.map(m => m.selectColumns()))
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

  filterMeasureFilters(measures) {
    return this.measureFilters.filter(f => R.any(m => m.measure === f.measure, measures));
  }

  keysQuery(primaryKeyDimension, filters) {
    const inlineWhereConditions = [];
    const query = this.rewriteInlineWhere(() => this.joinQuery(
      this.join,
      this.collectFrom(
        this.keyDimensions(primaryKeyDimension),
        this.collectSubQueryDimensionsFor.bind(this),
        'collectSubQueryDimensionsFor'
      )
    ), inlineWhereConditions);
    return `SELECT DISTINCT ${this.keysSelect(primaryKeyDimension)} FROM ${
      query
    } ${this.baseWhere(filters.concat(inlineWhereConditions))}`;
  }

  keysSelect(primaryKeyDimension) {
    return R.flatten(
      this.keyDimensions(primaryKeyDimension)
        .map(s => s.selectColumns())
    ).filter(s => !!s).join(', ');
  }

  keyDimensions(primaryKeyDimension) {
    return this.dimensionsForSelect()
      .concat(
        R.none(
          d => d.dimension === primaryKeyDimension.dimension,
          this.dimensionsForSelect()
        ) ? [primaryKeyDimension] : []
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
    const evaluatedSql = this.evaluateSql(cube, this.cubeEvaluator.cubeFromPath(cube).sql);
    const selectAsterisk = evaluatedSql.match(/^\s*select\s+\*\s+from\s+([a-zA-Z0-9_\-`".]+)\s*$/i);
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

  collectFromMembers(excludeTimeDimensions, fn, methodName) {
    const membersToCollectFrom = this.measures
      .concat(this.dimensions)
      .concat(this.segments)
      .concat(this.filters)
      .concat(this.measureFilters)
      .concat(excludeTimeDimensions ? [] : this.timeDimensions);
    return this.collectFrom(membersToCollectFrom, fn, methodName);
  }

  collectFrom(membersToCollectFrom, fn, methodName, cache) {
    return R.pipe(
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

  dimensionAliasNames() {
    return R.flatten(this.dimensionsForSelect().map(d => d.aliasName()).filter(d => !!d));
  }

  dimensionColumns(cubeAlias) {
    return this.dimensionAliasNames().map(alias => `${cubeAlias && `${cubeAlias}.` || ''}${alias}`);
  }

  groupByDimensionLimit() {
    const limitClause = this.rowLimit === null ? '' : ` LIMIT ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000}`;
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

  dimensionsForSelect() {
    return this.dimensions.concat(this.timeDimensions);
  }

  dimensionSql(dimension) {
    const context = this.safeEvaluateSymbolContext();
    if (context.rollupQuery) {
      return this.escapeColumnName(dimension.unescapedAliasName(context.rollupGranularity));
    }
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

  safeEvaluateSymbolContext() {
    return this.evaluateSymbolContext || {};
  }

  evaluateSymbolSql(cubeName, name, symbol) {
    this.pushCubeNameForCollectionIfNecessary(cubeName);
    if (this.cubeEvaluator.isMeasure([cubeName, name])) {
      let parentMeasure;
      if (this.safeEvaluateSymbolContext().compositeCubeMeasures ||
        this.safeEvaluateSymbolContext().leafMeasures) {
        parentMeasure = this.safeEvaluateSymbolContext().currentMeasure;
        if (this.safeEvaluateSymbolContext().compositeCubeMeasures) {
          if (parentMeasure &&
            (
              this.cubeEvaluator.cubeNameFromPath(parentMeasure) !== cubeName ||
              this.newMeasure(this.cubeEvaluator.pathFromArray([cubeName, name])).isCumulative()
            )
          ) {
            this.safeEvaluateSymbolContext().compositeCubeMeasures[parentMeasure] = true;
          }
        }
        this.safeEvaluateSymbolContext().currentMeasure = this.cubeEvaluator.pathFromArray([cubeName, name]);
        if (this.safeEvaluateSymbolContext().leafMeasures) {
          if (parentMeasure) {
            this.safeEvaluateSymbolContext().leafMeasures[parentMeasure] = false;
          }
          this.safeEvaluateSymbolContext().leafMeasures[this.safeEvaluateSymbolContext().currentMeasure] = true;
        }
      }
      const result = this.renderSqlMeasure(
        name,
        this.applyMeasureFilters(
          this.autoPrefixWithCubeName(
            cubeName,
            symbol.sql && this.evaluateSql(cubeName, symbol.sql) ||
            this.cubeEvaluator.primaryKeys[cubeName] && this.primaryKeySql(this.cubeEvaluator.primaryKeys[cubeName], cubeName) || '*'
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
    } else if (this.cubeEvaluator.isDimension([cubeName, name])) {
      if (symbol.subQuery) {
        const dimensionPath = this.cubeEvaluator.pathFromArray([cubeName, name]);
        if (this.safeEvaluateSymbolContext().subQueryDimensions) {
          this.safeEvaluateSymbolContext().subQueryDimensions.push(dimensionPath);
        }
        return this.escapeColumnName(this.aliasName(dimensionPath));
      }
      if (symbol.case) {
        return this.renderDimensionCase(symbol, cubeName);
      } else if (symbol.type === 'geo') {
        return this.concatStringsSql([
          this.autoPrefixAndEvaluateSql(cubeName, symbol.latitude.sql),
          "','",
          this.autoPrefixAndEvaluateSql(cubeName, symbol.longitude.sql)
        ]);
      } else {
        return this.autoPrefixAndEvaluateSql(cubeName, symbol.sql);
      }
    } else if (this.cubeEvaluator.isSegment([cubeName, name])) {
      return this.autoPrefixWithCubeName(cubeName, this.evaluateSql(cubeName, symbol.sql));
    }
    return this.evaluateSql(cubeName, symbol.sql);
  }

  autoPrefixAndEvaluateSql(cubeName, sql) {
    return this.autoPrefixWithCubeName(cubeName, this.evaluateSql(cubeName, sql));
  }

  concatStringsSql(strings) {
    return strings.join(" || ");
  }

  primaryKeyName(cubeName) {
    const primaryKey = this.cubeEvaluator.primaryKeys[cubeName];
    if (!primaryKey) {
      throw new UserError(`Primary key is required for '${cubeName}`);
    }
    return `${cubeName}.${primaryKey}`;
  }

  evaluateSql(cubeName, sql) {
    const self = this;
    const { cubeEvaluator } = this;
    this.pushCubeNameForCollectionIfNecessary(cubeName);
    return cubeEvaluator.resolveSymbolsCall(sql, (name) => {
      const nextCubeName = cubeEvaluator.symbols[name] && name || cubeName;
      this.pushCubeNameForCollectionIfNecessary(nextCubeName);
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
      sqlResolveFn: (symbol, cube, n) => self.evaluateSymbolSql(cube, n, symbol),
      cubeAliasFn: self.cubeAlias.bind(self),
      contextSymbols: this.parametrizedContextSymbols(),
      query: this
    });
  }

  withCubeAliasPrefix(cubeAliasPrefix, fn) {
    return this.evaluateSymbolSqlWithContext(fn, { cubeAliasPrefix });
  }

  cubeAlias(cubeName) {
    const prefix = this.safeEvaluateSymbolContext().cubeAliasPrefix || this.cubeAliasPrefix;
    return this.escapeColumnName(this.aliasName(`${prefix ? prefix + '__' : ''}${cubeName}`));
  }

  collectCubeNamesFor(fn) {
    const context = { cubeNames: [] };
    this.evaluateSymbolSqlWithContext(
      fn,
      context
    );
    return R.uniq(context.cubeNames);
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
    if ((this.safeEvaluateSymbolContext().ungroupedAliases || {})[measurePath]) {
      evaluateSql = (this.safeEvaluateSymbolContext().ungroupedAliases || {})[measurePath];
    }
    if ((this.safeEvaluateSymbolContext().ungroupedAliasesForCumulative || {})[measurePath]) {
      evaluateSql = (this.safeEvaluateSymbolContext().ungroupedAliasesForCumulative || {})[measurePath];
      const onGroupedColumn = this.aggregateOnGroupedColumn(symbol, evaluateSql);
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
    if (symbol.type === 'number') {
      return evaluateSql;
    }
    return `${symbol.type}(${evaluateSql})`;
  }

  aggregateOnGroupedColumn(symbol, evaluateSql) {
    if (symbol.type === 'count' || symbol.type === 'sum') {
      return `sum(${evaluateSql})`;
    } else if (symbol.type === 'countDistinctApprox') {
      return this.hllMerge(evaluateSql);
    } else if (symbol.type === 'min' || symbol.type === 'max') {
      return `${symbol.type}(${evaluateSql})`;
    }
    return undefined;
  }

  hllInit(sql) {
    throw new UserError(`Distributed approximate distinct count is not supported by this DB`);
  }

  hllMerge(sql) {
    throw new UserError(`Distributed approximate distinct count is not supported by this DB`);
  }

  countDistinctApprox(sql) {
    throw new UserError(`Approximate distinct count is not supported by this DB`);
  }

  primaryKeyCount(cubeName, distinct) {
    const primaryKeySql = this.primaryKeySql(this.cubeEvaluator.primaryKeys[cubeName], cubeName);
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
    ${when.map(w => `WHEN ${w.sql} THEN ${w.label}`).join("\n")}${elseLabel ? ` ELSE ${elseLabel}` : ''} END`;
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
    return this.inIntegrationTimeZone(date).clone().utc().format();
  }

  convertTz(field) {
    throw new Error('Not implemented');
  }

  timeGroupedColumn(granularity, dimension) {
    throw new Error('Not implemented');
  }

  aliasName(name, isPreAggregationName) {
    const path = name.split('.');
    if (path[0] && this.cubeEvaluator.cubeExists(path[0]) && this.cubeEvaluator.cubeFromPath(path[0]).sqlAlias) {
      const cubeName = path[0];
      path.splice(0, 1);
      path.unshift(this.cubeEvaluator.cubeFromPath(cubeName).sqlAlias);
      name = this.cubeEvaluator.pathFromArray(path);
    }
    // use single underscore for pre-aggregations to avoid fail of pre-aggregation name replace
    return inflection.underscore(name).replace(/\./g, isPreAggregationName ? '_' : '__');
  }

  newSubQuery(options) {
    const QueryClass = this.constructor;
    return new QueryClass(
      this.compilers,
      Object.assign({
        paramAllocator: this.paramAllocator,
        timezone: this.timezone,
        preAggregationQuery: this.options.preAggregationQuery,
        useOriginalSqlPreAggregationsInPreAggregation: this.options.useOriginalSqlPreAggregationsInPreAggregation,
        contextSymbols: this.contextSymbols,
        preAggregationsSchema: this.preAggregationsSchemaOption,
        cubeLatticeCache: this.options.cubeLatticeCache,
        historyQueries: this.options.historyQueries,
      }, options)
    );
  }

  cacheKeyQueries(transformFn) { // TODO collect sub queries
    if (!this.safeEvaluateSymbolContext().preAggregationQuery) {
      const preAggregationForQuery = this.preAggregations.findPreAggregationForQuery();
      if (preAggregationForQuery) {
        return {
          renewalThreshold: this.renewalThreshold(!!preAggregationForQuery.preAggregation.refreshKey),
          queries: []
        };
      }
    }

    return this.refreshKeysByCubes(this.allCubeNames, transformFn);
  }

  refreshKeysByCubes(cubes, transformFn) {
    let refreshKeyAllSetManually = true;
    const refreshKeyQueryByCube = cube => {
      const cubeFromPath = this.cubeEvaluator.cubeFromPath(cube);
      if (cubeFromPath.refreshKey) {
        if (cubeFromPath.refreshKey.sql) {
          return this.evaluateSql(cube, cubeFromPath.refreshKey.sql);
        }
        if (cubeFromPath.refreshKey.every) {
          return `SELECT ${this.everyRefreshKeySql(cubeFromPath.refreshKey.every)}`;
        }
      }
      refreshKeyAllSetManually = false;
      const timeDimensions =
        !(cubeFromPath.refreshKey && cubeFromPath.refreshKey.immutable) ?
          this.cubeEvaluator.timeDimensionPathsForCube(cube) :
          [];
      if (timeDimensions.length) {
        const dimension = timeDimensions.filter(f => f.toLowerCase().indexOf('update') !== -1)[0] || timeDimensions[0];
        const foundMainTimeDimension = this.newTimeDimension({ dimension });
        const aggSelect = this.aggSelectForDimension(cube, foundMainTimeDimension, 'max');
        if (aggSelect) {
          return aggSelect;
        }
      }
      return this.evaluateSymbolSqlWithContext(
        () => `select count(*) from ${this.cubeSql(cube)} ${this.asSyntaxTable} ${this.cubeAlias(cube)}`,
        { preAggregationQuery: true }
      );
    };
    const queries = cubes
      .map(cube => [cube, refreshKeyQueryByCube(cube)])
      .map(([cube, sql]) => (transformFn ? transformFn(sql, cube) : sql))
      .map(paramAnnotatedSql => this.paramAllocator.buildSqlAndParams(paramAnnotatedSql));
    return {
      queries,
      renewalThreshold: this.renewalThreshold(refreshKeyAllSetManually),
      refreshKeyRenewalThresholds: cubes.map(c => {
        const cubeFromPath = this.cubeEvaluator.cubeFromPath(c);
        if (cubeFromPath.refreshKey && cubeFromPath.refreshKey.every) {
          return this.refreshKeyRenewalThresholdForInterval(cubeFromPath.refreshKey.every);
        }
        return this.defaultRefreshKeyRenewalThreshold();
      })
    };
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
    return `NOW()`;
  }

  unixTimestampSql() {
    return `EXTRACT(EPOCH FROM ${this.nowTimestampSql()})`;
  }

  preAggregationTableName(cube, preAggregationName, skipSchema) {
    return `${skipSchema ? '' : this.preAggregationSchema() && `${this.preAggregationSchema()}.`}${this.aliasName(`${cube}.${preAggregationName}`, true)}`;
  }

  preAggregationSchema() {
    return this.preAggregationsSchemaOption;
  }

  preAggregationLoadSql(cube, preAggregation, tableName) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    return [`CREATE TABLE ${tableName} ${this.asSyntaxTable} ${sqlAndParams[0]}`, sqlAndParams[1]];
  }

  indexSql(cube, preAggregation, index, indexName, tableName) {
    if (preAggregation.external && this.externalQueryClass) {
      return this.externalQuery().indexSql(cube, preAggregation, index, indexName, tableName);
    }
    if (index.columns) {
      const columns = this.cubeEvaluator.evaluateReferences(cube, index.columns, { originalSorting: true });
      const escapedColumns = columns.map(column => {
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
      return this.paramAllocator.buildSqlAndParams(this.createIndexSql(indexName, tableName, escapedColumns));
    } else {
      throw new Error(`Index SQL support is not implemented`);
    }
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

  everyRefreshKeySql(interval) {
    return this.floorSql(`${this.unixTimestampSql()} / ${this.parseSecondDuration(interval)}`);
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

  parseSecondDuration(interval) {
    const intervalMatch = interval.match(/^(\d+) (second|minute|hour|day|week)s?$/);
    if (!intervalMatch) {
      throw new UserError(`Invalid interval: ${interval}`);
    }
    const duration = parseInt(intervalMatch[1], 10);
    if (duration < 1) {
      throw new UserError(`Duration should be positive: ${interval}`);
    }
    const secondsInInterval = SecondsDurations[intervalMatch[2]];
    return secondsInInterval * duration;
  }

  floorSql(numeric) {
    return `FLOOR(${numeric})`;
  }

  incrementalRefreshKey(query, originalRefreshKey, options) {
    options = options || {};
    const updateWindow = options.window;
    return query.evaluateSql(
      null,
      (FILTER_PARAMS) => query.caseWhenStatement([{
        sql: FILTER_PARAMS[
          query.timeDimensions[0].path()[0]
        ][
          query.timeDimensions[0].path()[1]
        ].filter((from, to) => `${query.nowTimestampSql()} < ${updateWindow ? this.addTimestampInterval(this.timeStampCast(to), updateWindow) : this.timeStampCast(to)}`),
        label: originalRefreshKey
      }])
    );
  }

  defaultRefreshKeyRenewalThreshold() {
    return 10;
  }

  preAggregationInvalidateKeyQueries(cube, preAggregation) {
    return this.cacheValue(
      ['preAggregationInvalidateKeyQueries', cube, JSON.stringify(preAggregation)],
      () => {
        const preAggregationQueryForSql = this.preAggregationQueryForSqlEvaluation(cube, preAggregation);
        if (preAggregation.refreshKey) {
          if (preAggregation.refreshKey.sql) {
            return {
              queries: [this.paramAllocator.buildSqlAndParams(
                preAggregationQueryForSql.evaluateSql(cube, preAggregation.refreshKey.sql)
              )],
              refreshKeyRenewalThresholds: [this.defaultRefreshKeyRenewalThreshold()]
            };
          }
          const interval = preAggregation.refreshKey.every || `1 hour`;
          let refreshKey = this.everyRefreshKeySql(interval);
          if (preAggregation.refreshKey.incremental) {
            if (!preAggregation.partitionGranularity) {
              throw new UserError(`Incremental refresh key can only be used for partitioned pre-aggregations`);
            }
            // TODO Case when partitioned originalSql is resolved for query without time dimension.
            // Consider fallback to not using such originalSql for consistency?
            if (preAggregationQueryForSql.timeDimensions.length) {
              refreshKey = this.incrementalRefreshKey(
                preAggregationQueryForSql,
                refreshKey,
                { window: preAggregation.refreshKey.updateWindow }
              );
            }
          }
          if (preAggregation.refreshKey.every || preAggregation.refreshKey.incremental) {
            return {
              queries: [this.paramAllocator.buildSqlAndParams(`SELECT ${refreshKey}`)],
              refreshKeyRenewalThresholds: [this.refreshKeyRenewalThresholdForInterval(interval)]
            };
          }
        }
        if (preAggregation.type === 'originalSql') {
          return this.evaluateSymbolSqlWithContext(
            () => this.refreshKeysByCubes([cube]),
            { preAggregationQuery: true }
          );
        }
        if (
          preAggregation.partitionGranularity &&
          !preAggregationQueryForSql.allCubeNames.find(c => {
            const fromPath = this.cubeEvaluator.cubeFromPath(c);
            return fromPath.refreshKey && fromPath.refreshKey.sql;
          })
        ) {
          const cubeFromPath = this.cubeEvaluator.cubeFromPath(cube);
          return preAggregationQueryForSql.evaluateSymbolSqlWithContext(
            () => preAggregationQueryForSql.cacheKeyQueries(
              (originalRefreshKey, refreshKeyCube) => {
                if (cubeFromPath.refreshKey && cubeFromPath.refreshKey.immutable) {
                  return `SELECT ${this.incrementalRefreshKey(preAggregationQueryForSql, `(${originalRefreshKey})`)}`;
                } else if (!cubeFromPath.refreshKey) {
                  // TODO handle WHERE while generating originalRefreshKey
                  return refreshKeyCube === preAggregationQueryForSql.timeDimensions[0].path()[0] ?
                    `${originalRefreshKey} WHERE ${preAggregationQueryForSql.timeDimensions[0].filterToWhere()}` :
                    originalRefreshKey;
                }
                return originalRefreshKey;
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

  refreshKeyRenewalThresholdForInterval(interval) {
    return Math.max(Math.min(Math.round(this.parseSecondDuration(interval) / 10), 300), 1);
  }

  preAggregationStartEndQueries(cube, preAggregation) {
    const references = this.cubeEvaluator.evaluatePreAggregationReferences(cube, preAggregation);
    const timeDimension = this.newTimeDimension(references.timeDimensions[0]);

    return this.evaluateSymbolSqlWithContext(() => [
      this.paramAllocator.buildSqlAndParams(this.aggSelectForDimension(timeDimension.path()[0], timeDimension, 'min')),
      this.paramAllocator.buildSqlAndParams(this.aggSelectForDimension(timeDimension.path()[0], timeDimension, 'max'))
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

  contextSymbolsProxy(symbols) {
    return new Proxy(symbols, {
      get: (target, name) => {
        const propValue = target[name];
        const methods = (paramValue) => ({
          filter: (column) => {
            if (paramValue) {
              const value = Array.isArray(paramValue) ?
                paramValue.map(this.paramAllocator.allocateParam.bind(this.paramAllocator)) :
                this.paramAllocator.allocateParam(paramValue);
              if (typeof column === "function") {
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
            return methods.filter(column);
          },
          unsafeValue: () => paramValue
        });
        return methods(target)[name] ||
          typeof propValue === 'object' && this.contextSymbolsProxy(propValue) ||
          methods(propValue);
      }
    });
  }

  filtersProxy() {
    const { allFilters } = this;
    return new Proxy({}, {
      get: (target, name) => {
        if (name === '_objectWithResolvedProperties') {
          return true;
        }
        const cubeName = this.cubeEvaluator.cubeNameFromPath(name);
        return new Proxy({ cube: cubeName }, {
          get: (cubeNameObj, propertyName) => {
            const filter =
              allFilters.find(f => f.dimension === this.cubeEvaluator.pathFromArray([cubeNameObj.cube, propertyName]));
            return {
              filter: (column) => {
                const filterParams = filter && filter.filterParams();
                if (
                  filterParams && filterParams.length
                ) {
                  if (typeof column === "function") {
                    // eslint-disable-next-line prefer-spread
                    return column.apply(
                      null,
                      filterParams.map(this.paramAllocator.allocateParam.bind(this.paramAllocator))
                    );
                  } else {
                    return filter.conditionSql(column);
                  }
                } else {
                  return '1 = 1';
                }
              }
            };
          }
        });
      }
    });
  }
}

module.exports = BaseQuery;
