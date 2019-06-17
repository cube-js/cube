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

const DEFAULT_PREAGGREGATIONS_SCHEMA = `stb_pre_aggregations`;

class BaseQuery {
  constructor(compilers, options) {
    this.compilers = compilers;
    this.cubeEvaluator = compilers.cubeEvaluator;
    this.joinGraph = compilers.joinGraph;
    this.options = options || {};

    this.orderHashToString = this.orderHashToString.bind(this);
    this.defaultOrder = this.defaultOrder.bind(this);

    this.initFromOptions();
  }

  initFromOptions() {
    this.contextSymbols = Object.assign({ userContext: {} }, this.options.contextSymbols || {});
    this.paramAllocator = this.options.paramAllocator || this.newParamAllocator();
    this.timezone = this.options.timezone;
    this.rowLimit = this.options.rowLimit;
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

        dimension.dimension = this.cubeEvaluator.timeDimensionPathsForCube(join.root)[0];
        if (!dimension.dimension) {
          return undefined;
        }
      }
      return dimension;
    }).filter(R.identity).map(this.newTimeDimension.bind(this));
    this.allFilters = this.timeDimensions.concat(this.segments).concat(this.filters);
    this.join = this.joinGraph.buildJoin(this.collectCubeNames());
    this.cubeAliasPrefix = this.options.cubeAliasPrefix;
    this.preAggregationsSchemaOption =
      this.options.preAggregationsSchema != null ? this.options.preAggregationsSchema : DEFAULT_PREAGGREGATIONS_SCHEMA;

    if (this.order.length === 0) {
      this.order = this.defaultOrder();
    }

    this.externalQueryClass = this.options.externalQueryClass;
  }

  get subQueryDimensions() {
    if (!this._subQueryDimensions) {
      this._subQueryDimensions = this.collectFromMembers(false, this.collectSubQueryDimensionsFor.bind(this));
    }
    return this._subQueryDimensions;
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
    if (!this.options.preAggregationQuery) {
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
    }
    return false;
  }

  buildSqlAndParams() {
    if (!this.options.preAggregationQuery && this.externalQueryClass) {
      const preAggregationForQuery = this.preAggregations.findPreAggregationForQuery();
      if (preAggregationForQuery && preAggregationForQuery.preAggregation.external) {
        const ExternalQuery = this.externalQueryClass;
        return new ExternalQuery(this.compilers, {
          ...this.options,
          externalQueryClass: null
        }).buildSqlAndParams();
      }
    }
    return this.compilers.compiler.withQuery(
      this,
      () => this.paramAllocator.buildSqlAndParams(this.buildParamAnnotatedSql())
    );
  }

  runningTotalDateJoinCondition() {
    return this.timeDimensions.map(d =>
      [d, (dateFrom, dateTo, dateField, dimensionDateFrom, dimensionDateTo) =>
        `${dateField} >= ${dimensionDateFrom} AND ${dateField} <= ${dateTo}`
      ]
    );
  }

  rollingWindowDateJoinCondition(trailingInterval, leadingInterval, offset) {
    offset = offset || 'end';
    return this.timeDimensions.map(d =>
      [d, (dateFrom, dateTo, dateField, dimensionDateFrom, dimensionDateTo, isFromStartToEnd) => {
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

  cumulativeMeasures() {
    return this.measures.filter(m => m.isCumulative());
  }

  isRolling() {
    return !!this.measures.find(m => m.isRolling()); // TODO
  }

  simpleQuery() {
    // eslint-disable-next-line prefer-template
    return `${this.commonQuery()} ${this.baseWhere(this.allFilters)}` +
      this.groupByClause() +
      this.baseHaving(this.measureFilters) +
      this.orderBy() +
      this.groupByDimensionLimit();
  }

  fullKeyQueryAggregate() {
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
            ([keyCubeName, measures]) =>
              this.withCubeAliasPrefix(`${keyCubeName}_key`, () => this.aggregateSubQuery(keyCubeName, measures))
          )
        )(multipliedMeasures)
      ).concat(
        R.map(
          ([multiplied, measure]) =>
            this.withCubeAliasPrefix(
              `${this.aliasName(measure.measure.replace('.', '_'))}_cumulative`,
              () => this.overTimeSeriesQuery(
                multiplied ?
                  (measures, filters) =>
                    this.aggregateSubQuery(measures[0].cube().name, measures, filters)
                  : this.regularMeasuresSubQuery.bind(this),
                measure
              )
            )
        )(cumulativeMeasures)
      );

    const join = R.drop(1, toJoin)
      .map((q, i) =>
        (this.dimensionAliasNames().length ?
          `INNER JOIN (${q}) as q_${i + 1} ON ${this.dimensionsJoinCondition(`q_${i}`, `q_${i + 1}`)}` :
          `, (${q}) as q_${i + 1}`)
      )
      .join("\n");

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

  minGranularity(granularityA, granularityB) {
    if (!granularityA) {
      return granularityB;
    }
    if (!granularityB) {
      return granularityA;
    }
    if (granularityA === 'hour' || granularityB === 'hour') {
      return 'hour';
    } else if (granularityA === 'date' || granularityB === 'date') {
      return 'date';
    } else if (granularityA === 'month' && granularityB === 'month') {
      return 'month';
    } else if (granularityA === 'year' && granularityB === 'year') {
      return 'year';
    } else if (granularityA === 'week' && granularityB === 'week') {
      return 'week';
    }
    return 'date';
  }

  overTimeSeriesQuery(baseQueryFn, cumulativeMeasure) {
    const dateJoinCondition = cumulativeMeasure.dateJoinCondition();
    const cumulativeMeasures = [cumulativeMeasure];
    const dateFromStartToEndConditionSql = (isFromStartToEnd) =>
      dateJoinCondition.map(([d, f]) =>
        // TODO these weird conversions to be strict typed for big query.
        // TODO Consider adding strict definitions of local and UTC time type
        ({
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
      dateJoinCondition.map(([d, f]) =>
        f(
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
      ` LEFT JOIN (${baseQuery}) AS ${baseQueryAlias} ON ${dateJoinConditionSql}` +
      this.groupByClause();
  }

  dateSeriesSelect() {
    return this.timeDimensions.map(d => d.dateSeriesSelectColumn());
  }

  dateSeriesSql(timeDimension) {
    return `(${this.seriesSql(timeDimension)}) AS ${timeDimension.dateSeriesAliasName()}`;
  }

  seriesSql(timeDimension) {
    const values = timeDimension.timeSeries().map(
      ([from, to]) => `('${from}', '${to}')`
    );
    return `SELECT date_from::timestamptz, date_to::timestamptz FROM (VALUES ${values}) AS dates (date_from, date_to)`;
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
      const collectedMeasures = this.collectMultipliedMeasures(() => this.traverseSymbol(m));
      if (m.expressionName && collectedMeasures.length === 1 && !collectedMeasures[0]) {
        throw new UserError(`Subquery dimension ${m.expressionName} should reference at least one measure`);
      }
      return [m.measure, collectedMeasures];
    }));
  }

  query() {
    return this.joinQuery(this.join, this.collectFromMembers(false, this.collectSubQueryDimensionsFor.bind(this)));
  }

  joinQuery(join, subQueryDimensions) {
    const joins = join.joins.map(j =>
      `LEFT JOIN ${this.cubeSql(j.originalTo)} AS ${this.cubeAlias(j.originalTo)}
      ON ${this.evaluateSql(j.originalFrom, j.join.sql)}`
    ).concat(subQueryDimensions.map(d => this.subQueryJoin(d)));

    return `${this.cubeSql(join.root)} AS ${this.cubeAlias(join.root)}\n${joins.join("\n")}`;
  }

  subQueryJoin(dimension) {
    const { prefix, subQuery, cubeName } = this.subQueryDescription(dimension);
    const primaryKey = this.newDimension(this.primaryKeyName(cubeName));
    const subQueryAlias = this.escapeColumnName(this.aliasName(prefix));
    return `LEFT JOIN (${subQuery.buildParamAnnotatedSql()}) AS ${subQueryAlias}
    ON ${subQueryAlias}.${primaryKey.aliasName()} = ${this.primaryKeySql(this.cubeEvaluator.primaryKeys[cubeName], cubeName)}`;
  }

  subQueryDescription(dimension) {
    const symbol = this.cubeEvaluator.dimensionByPath(dimension);
    const [cubeName, name] = this.cubeEvaluator.parsePath('dimensions', dimension);
    const prefix = this.subQueryName(cubeName, name);
    const subQuery = this.newSubQuery({
      cubeAliasPrefix: prefix,
      rowLimit: null,
      measures: [{
        expression: symbol.sql,
        cubeName: cubeName,
        name
      }],
      dimensions: [this.primaryKeyName(cubeName)]
    });
    return { prefix, subQuery, cubeName };
  }

  subQueryName(cubeName, name) {
    return `${cubeName}_${name}_subquery`;
  }

  regularMeasuresSubQuery(measures, filters) {
    filters = filters || this.allFilters;

    return `SELECT ${this.selectAllDimensionsAndMeasures(measures)} FROM ${
      this.joinQuery(
        this.join,
        this.collectFrom(
          this.dimensionsForSelect().concat(measures).concat(this.allFilters),
          this.collectSubQueryDimensionsFor.bind(this)
        )
      )
    } ${this.baseWhere(filters)}` +
    (!this.safeEvaluateSymbolContext().ungrouped && this.groupByClause() || '');
  }

  aggregateSubQuery(keyCubeName, measures, filters) {
    filters = filters || this.allFilters;
    const primaryKeyDimension = this.newDimension(this.primaryKeyName(keyCubeName));
    const shouldBuildJoinForMeasureSelect = this.checkShouldBuildJoinForMeasureSelect(measures, keyCubeName);

    let keyCubeSql = this.cubeSql(keyCubeName);

    const measureSubQueryDimensions = this.collectFrom(measures, this.collectSubQueryDimensionsFor.bind(this));

    if (shouldBuildJoinForMeasureSelect) {
      const cubes = this.collectCubeNamesFor(() => measures.map(m => this.traverseSymbol(m)));
      const measuresJoin = this.joinGraph.buildJoin(cubes);
      if (measuresJoin.multiplicationFactor[keyCubeName]) {
        throw new UserError(
          `'${measures.map(m => m.measure).join(', ')}' reference cubes that lead to row multiplication.`
        );
      }
      keyCubeSql = `(${this.aggregateSubQueryMeasureJoin(keyCubeName, measures, measuresJoin, primaryKeyDimension, measureSubQueryDimensions)})`;
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
    return `SELECT ${columnsForSelect} FROM (${this.keysQuery(primaryKeyDimension, filters)}) AS ${this.escapeColumnName('keys')} ` +
      `LEFT OUTER JOIN ${keyCubeSql} AS ${this.cubeAlias(keyCubeName)} ON
      ${this.escapeColumnName('keys')}.${primaryKeyDimension.aliasName()} = ${keyInMeasureSelect} ` +
      subQueryJoins +
      (!this.safeEvaluateSymbolContext().ungrouped && this.groupByClause() || '');
  }

  checkShouldBuildJoinForMeasureSelect(measures, keyCubeName) {
    return measures.map(measure => {
      const cubeNames = this.collectCubeNamesFor(() => this.traverseSymbol(measure));
      if (R.any(cubeName => keyCubeName !== cubeName, cubeNames)) {
        const cubes = this.collectCubeNamesFor(() => this.traverseSymbol(measure));
        const measuresJoin = this.joinGraph.buildJoin(cubes);
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
      }
    ));
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
    return `SELECT DISTINCT ${this.keysSelect(primaryKeyDimension)} FROM ${
      this.joinQuery(
        this.join,
        this.collectFrom(this.keyDimensions(primaryKeyDimension), this.collectSubQueryDimensionsFor.bind(this))
      )
    } ${this.baseWhere(filters)}`;
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
      if (this.options.collectOriginalSqlPreAggregations) {
        this.options.collectOriginalSqlPreAggregations.push(foundPreAggregation);
      }
      return this.preAggregationTableName(cube, foundPreAggregation.preAggregationName);
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
    return this.collectFromMembers(excludeTimeDimensions, this.collectCubeNamesFor.bind(this));
  }

  collectFromMembers(excludeTimeDimensions, fn) {
    const membersToCollectFrom = this.measures
      .concat(this.dimensions)
      .concat(this.segments)
      .concat(this.filters)
      .concat(this.measureFilters)
      .concat(excludeTimeDimensions ? [] : this.timeDimensions);
    return this.collectFrom(membersToCollectFrom, fn);
  }

  collectFrom(membersToCollectFrom, fn) {
    return R.pipe(
      R.map(s =>
          fn(() => this.traverseSymbol(s))
      ),
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

  groupByClause() {
    const dimensionColumns = this.dimensionColumns();
    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.map((c, i) => `${i + 1}`).join(', ')}` : '';
  }

  getFieldIndex(id) {
    const equalIgnoreCase = (a, b) => (
      typeof a === 'string' && typeof b === 'string'
      && a.toUpperCase() === b.toUpperCase()
    );

    let index;

    index = this.dimensionsForSelect().findIndex(d =>
      equalIgnoreCase(d.dimension, id)
    );

    if (index > -1) {
      return index + 1;
    }

    index = this.measures.findIndex(d =>
      equalIgnoreCase(d.measure, id) || equalIgnoreCase(d.expressionName, id)
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
    return this.rowLimit === null ? '' : ` LIMIT ${this.rowLimit && parseInt(this.rowLimit, 10) || 10000}`;
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
    if (this.safeEvaluateSymbolContext().rollupQuery) {
      return this.escapeColumnName(dimension.unescapedAliasName());
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
        return this.renderDimensionCase(symbol, cubeName)
      } else if (symbol.type === 'geo') {
        return this.concatStringsSql([
          this.autoPrefixAndEvaluateSql(cubeName, symbol.latitude.sql),
          "','",
          this.autoPrefixAndEvaluateSql(cubeName, symbol.longitude.sql)
        ])
      } else {
        return this.autoPrefixAndEvaluateSql(cubeName, symbol.sql)
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
    const cubeEvaluator = this.cubeEvaluator;
    this.pushCubeNameForCollectionIfNecessary(cubeName);
    return cubeEvaluator.resolveSymbolsCall(sql, (name) => {
      const nextCubeName = cubeEvaluator.symbols[name] && name || cubeName;
      this.pushCubeNameForCollectionIfNecessary(nextCubeName);
      const resolvedSymbol =
        cubeEvaluator.resolveSymbol(
          cubeName,
          name
        );
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

  // TODO merge fail. Remove sqlAlias us unused?
  /*
  cubeAlias(cube) {
    return this.cubeAlias(this.cubeEvaluator.cubeFromPath(cube).sqlAlias || cube);
  }
  */

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
    const context = { compositeCubeMeasures: {} };
    this.evaluateSymbolSqlWithContext(
      fn,
      context
    );
    const foundCompositeCubeMeasures = context.compositeCubeMeasures;

    const renderContext = { measuresToRender: [], foundCompositeCubeMeasures, compositeCubeMeasures: {} };
    this.evaluateSymbolSqlWithContext(
      fn,
      renderContext
    );
    return renderContext.measuresToRender.length ? R.uniq(renderContext.measuresToRender) : [renderContext.rootMeasure];
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
      this.safeEvaluateSymbolContext().rootMeasure = { multiplied: resultMultiplied, measure: measurePath };
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
    const when = symbol.case.when.map(w => ({ sql: this.evaluateSql(cubeName, w.sql), label: w.label }));
    return this.caseWhenStatement(when, symbol.case.else && symbol.case.else.label);
  }

  caseWhenStatement(when, elseLabel) {
    return `CASE
    ${when.map(w => `WHEN ${w.sql} THEN '${w.label}'`).join("\n")}${elseLabel ? ` ELSE '${elseLabel}'` : ''} END`;
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

  aliasName(name) {
    return inflection.underscore(name);
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
        collectOriginalSqlPreAggregations: this.options.collectOriginalSqlPreAggregations,
        contextSymbols: this.contextSymbols,
        preAggregationsSchema: this.preAggregationsSchemaOption,
        cubeLatticeCache: this.options.cubeLatticeCache,
        historyQueries: this.options.historyQueries,
      }, options)
    );
  }

  cacheKeyQueries() { // TODO collect sub queries
    const preAggregationForQuery = this.preAggregations.findPreAggregationForQuery();
    if (preAggregationForQuery) {
      return {
        renewalThreshold: this.renewalThreshold(!!preAggregationForQuery.refreshKey),
        queries: this.preAggregationInvalidateKeyQueries(preAggregationForQuery.cube, preAggregationForQuery)
      };
    }
    let refreshKeyAllSetManually = true;
    const queries = this.collectCubeNames()
      .map(cube => {
        const cubeFromPath = this.cubeEvaluator.cubeFromPath(cube);
        if (cubeFromPath.refreshKey) {
          return this.evaluateSql(cube, cubeFromPath.refreshKey.sql);
        }
        refreshKeyAllSetManually = false;
        const timeDimensions = this.cubeEvaluator.timeDimensionPathsForCube(cube);
        if (timeDimensions.length) {
          const dimension = timeDimensions.filter(f => f.toLowerCase().indexOf('update') !== -1)[0] || timeDimensions[0];
          const foundMainTimeDimension = this.newTimeDimension({ dimension });
          const cubeNamesForTimeDimension = this.collectFrom(
            [foundMainTimeDimension],
            this.collectCubeNamesFor.bind(this)
          );
          if (cubeNamesForTimeDimension.length === 1 && cubeNamesForTimeDimension[0] === cube) {
            const dimensionSql = this.dimensionSql(foundMainTimeDimension);
            return `select max(${dimensionSql}) from ${this.cubeSql(cube)} AS ${this.cubeAlias(cube)}`;
          }
        }
        return `select count(*) from ${this.cubeSql(cube)} AS ${this.cubeAlias(cube)}`;
      }).map(paramAnnotatedSql => this.paramAllocator.buildSqlAndParams(paramAnnotatedSql));
    return {
      queries,
      renewalThreshold: this.renewalThreshold(refreshKeyAllSetManually)
    };
  }

  cubeCardinalityQueries() { // TODO collect sub queries
    return R.fromPairs(this.collectCubeNames()
      .map(cube => [
        cube,
        this.paramAllocator.buildSqlAndParams(`select count(*) as ${this.escapeColumnName('total_count')} from ${this.cubeSql(cube)} AS ${this.cubeAlias(cube)}`)
      ])
    );
  }

  renewalThreshold(refreshKeyAllSetManually) {
    return refreshKeyAllSetManually ? 24 * 60 * 60 : 6 * 60 * 60;
  }

  nowTimestampSql() {
    return `NOW()`;
  }

  preAggregationTableName(cube, preAggregationName) {
    return `${this.preAggregationSchema() && `${this.preAggregationSchema()}.`}${this.aliasName(`${cube}_${preAggregationName}`)}`;
  }

  preAggregationSchema() {
    return this.preAggregationsSchemaOption;
  }

  preAggregationLoadSql(cube, preAggregation, tableName) {
    const sqlAndParams = this.preAggregationSql(cube, preAggregation);
    return [`CREATE TABLE ${tableName} AS ${sqlAndParams[0]}`, sqlAndParams[1]];
  }

  preAggregationSql(cube, preAggregation) {
    if (preAggregation.type === 'autoRollup') {
      return this.preAggregations.autoRollupPreAggregationQuery(cube, preAggregation).buildSqlAndParams();
    } else if (preAggregation.type === 'rollup') {
      return this.preAggregations.rollupPreAggregationQuery(cube, preAggregation).buildSqlAndParams();
    } else if (preAggregation.type === 'originalSql') {
      return [
        this.evaluateSymbolSqlWithContext(
        () => this.evaluateSql(cube, this.cubeEvaluator.cubeFromPath(cube).sql),
        { preAggregationQuery: true }
        ),
        []
      ];
    }
    throw new UserError(`Unknown pre-aggregation type '${preAggregation.type}' in '${cube}'`);
  }

  preAggregationQueryForSqlEvaluation(cube, preAggregation) {
    if (preAggregation.type === 'autoRollup') {
      return this.preAggregations.autoRollupPreAggregationQuery(cube, preAggregation);
    } else if (preAggregation.type === 'rollup') {
      return this.preAggregations.rollupPreAggregationQuery(cube, preAggregation);
    } else if (preAggregation.type === 'originalSql') {
      return this;
    }
  }

  preAggregationInvalidateKeyQueries(cube, preAggregation) {
    if (preAggregation.refreshKey) {
      return [this.paramAllocator.buildSqlAndParams(
        this.preAggregationQueryForSqlEvaluation(cube, preAggregation).evaluateSql(cube, preAggregation.refreshKey.sql)
      )];
    }
    return [this.paramAllocator.buildSqlAndParams(
      `SELECT ${this.timeGroupedColumn('hour', this.convertTz(this.nowTimestampSql()))} as current_hour`
    )];
  }

  parametrizedContextSymbols() {
    return Object.assign({
      filterParams: this.filtersProxy(),
      sqlUtils: {
        convertTz: this.convertTz.bind(this)
      }
    }, R.map(
      (symbols) => this.contextSymbolsProxy(symbols),
      this.contextSymbols
    ));
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
          }
        });
        return methods(target)[name] ||
          typeof propValue === 'object' && this.contextSymbolsProxy(propValue) ||
          methods(propValue);
      }
    });
  }

  filtersProxy() {
    const allFilters = this.allFilters;
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
                if (filter && filter.filterParams() && filter.filterParams().length) {
                  if (typeof column === "function") {
                    return column.apply(
                      null,
                      filter.filterParams().map(this.paramAllocator.allocateParam.bind(this.paramAllocator))
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
