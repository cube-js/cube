import inlection from 'inflection';
import * as momentRange from 'moment-range';
import { contains, join, map } from 'ramda';
import { FROM_PARTITION_RANGE, TO_PARTITION_RANGE } from '@cubejs-backend/shared';

import { BaseDimension } from './BaseDimension';

const moment = momentRange.extendMoment(require('moment-timezone'));

const DATE_OPERATORS = ['inDateRange', 'notInDateRange', 'onTheDate', 'beforeDate', 'beforeOrOnDate', 'afterDate', 'afterOrOnDate'];
const dateTimeLocalMsRegex = /^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d$/;
const dateRegex = /^\d\d\d\d-\d\d-\d\d$/;

export class BaseFilter extends BaseDimension {
  constructor(query, filter) {
    super(query, filter.dimension);
    this.measure = filter.measure;
    this.query = query;
    this.operator = filter.operator;
    this.values = filter.values;
    this.camelizeOperator = inlection.camelize(this.operator).replace(
      /[A-Z]/,
      (c) => (c != null ? c : '').toLowerCase(),
    );
  }

  filterToWhere() {
    if (this.camelizeOperator === 'measureFilter') {
      return this.measureFilterToWhere();
    }

    return this.conditionSql(this.measure ? this.query.measureSql(this) : this.query.dimensionSql(this));
  }

  convertTzForRawTimeDimensionIfNeeded(sql) {
    return sql();
  }

  // Evaluates filters on measures to whole where statement in query
  // It used in drill downs
  measureFilterToWhere() {
    const measureDefinition = this.measureDefinition();
    if (measureDefinition.filters && measureDefinition.filters.length ||
      measureDefinition.drillFilters && measureDefinition.drillFilters.length) {
      return this.query.evaluateFiltersArray(
        (measureDefinition.filters || []).concat(measureDefinition.drillFilters || []),
        this.query.cubeEvaluator.cubeNameFromPath(this.measure)
      );
    } else {
      return null;
    }
  }

  conditionSql(columnSql) {
    const operatorMethod = `${(this.camelizeOperator)}Where`;

    let sql = this[operatorMethod](columnSql);
    if (sql.match(this.query.paramAllocator.paramsMatchRegex)) {
      return sql;
    }
    // TODO DEPRECATED: remove and replace with error
    // columnSql can contain `?` so allocate params first and then substitute columnSql
    // fallback implementation for drivers that still use `?` substitution
    sql = this[operatorMethod]('$$$COLUMN$$$');
    return this.query.paramAllocator.allocateParamsForQuestionString(sql, this.filterParams()).replace(/\$\$\$COLUMN\$\$\$/g, columnSql);
  }

  measureDefinition() {
    return this.query.cubeEvaluator.measureByPath(this.measure);
  }

  path() {
    if (this.measure) {
      return this.query.cubeEvaluator.parsePath('measures', this.measure);
    } else if (this.query.cubeEvaluator.isInstanceOfType('segments', this.dimension)) {
      return this.query.cubeEvaluator.parsePath('segments', this.dimension);
    } else {
      return this.query.cubeEvaluator.parsePath('dimensions', this.dimension);
    }
  }

  cube() {
    return this.query.cubeEvaluator.cubeFromPath(this.measure || this.dimension);
  }

  definition() {
    return this.measure ? this.measureDefinition() : this.dimensionDefinition();
  }

  // noinspection JSMethodCanBeStatic
  escapeWildcardChars(param) {
    return typeof param === 'string' ? param.replace(/([_%])/gi, '\\$1') : param;
  }

  isWildcardOperator() {
    return this.camelizeOperator === 'contains' || this.camelizeOperator === 'notContains';
  }

  filterParams() {
    if (this.isDateOperator()) {
      return [this.inDbTimeZoneDateFrom(this.values[0]), this.inDbTimeZoneDateTo(this.values[1])];
    }
    if (this.camelizeOperator === 'set' || this.camelizeOperator === 'notSet' || this.camelizeOperator === 'expressionEquals') {
      return [];
    }
    const params = this.valuesArray().filter(v => v != null);

    if (this.isWildcardOperator()) {
      return map(this.escapeWildcardChars, params);
    }

    return params;
  }

  isDateOperator() {
    return contains(this.camelizeOperator, DATE_OPERATORS);
  }

  valuesArray() {
    return Array.isArray(this.values) ? this.values : [this.values];
  }

  valuesContainNull() {
    return this.valuesArray().indexOf(null) !== -1;
  }

  castParameter() {
    return '?';
  }

  firstParameter() {
    const params = this.filterParams();
    if (!params.length) {
      throw new Error('Expected one parameter but nothing found');
    }

    return this.allocateCastParam(params[0]);
  }

  allocateCastParam(param) {
    return this.query.paramAllocator.allocateParamsForQuestionString(this.castParameter(), [param]);
  }

  allocateTimestampParam(param) {
    return this.query.paramAllocator.allocateParamsForQuestionString(this.query.timeStampParam(this), [param]);
  }

  allocateTimestampParams() {
    return this.filterParams().map((p, i) => {
      if (i > 1) {
        throw new Error(`Expected only 2 parameters for timestamp filter but got: ${this.filterParams()}`);
      }
      return this.allocateTimestampParam(p);
    });
  }

  allParamsRepeat(basePart) {
    return this.filterParams().map(p => this.query.paramAllocator.allocateParamsForQuestionString(basePart, [p]));
  }

  isArrayValues() {
    return Array.isArray(this.values) && this.values.length > 1;
  }

  containsWhere(column) {
    return this.likeOr(column, false, 'contains');
  }

  notContainsWhere(column) {
    return this.likeOr(column, true, 'contains');
  }

  /**
   * Returns SQL statement for the `startsWith` filter.
   * @param {string} column Column name.
   * @returns string
   */
  startsWithWhere(column) {
    return this.likeOr(column, false, 'starts');
  }

  /**
   * Returns SQL statement for the `notStartsWith` filter.
   * @param {string} column Column name.
   * @returns string
   */
  notStartsWithWhere(column) {
    return this.likeOr(column, true, 'starts');
  }

  /**
   * Returns SQL statement for the `endsWith` filter.
   * @param {string} column Column name.
   * @returns string
   */
  endsWithWhere(column) {
    return this.likeOr(column, false, 'ends');
  }

  /**
   * Returns SQL statement for the `endsWith` filter.
   * @param {string} column Column name.
   * @returns string
   */
  notEndsWithWhere(column) {
    return this.likeOr(column, true, 'ends');
  }

  /**
   * Returns SQL filter statement (union with the logical OR) for the
   * provided parameters.
   * @param {string} column Column name.
   * @param {boolean} not Flag to build NOT LIKE statement.
   * @param {string} type Type of the condition (i.e. contains/
   * startsWith/endsWith).
   * @returns string
   */
  likeOr(column, not, type) {
    type = type || 'contains';
    return `${join(not ? ' AND ' : ' OR ', this.filterParams().map(
      p => this.likeIgnoreCase(column, not, p, type)
    ))}${this.orIsNullCheck(column, not)}`;
  }

  /**
   * Returns SQL LIKE statement for specified parameters.
   * @param {string} column Column name.
   * @param {boolean} not Flag to build NOT LIKE statement.
   * @param {*} param Value for statement.
   * @param {string} type Type of the condition (i.e. contains/startsWith/endsWith).
   * @returns string
   */
  likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    return `${column}${not ? ' NOT' : ''} ILIKE ${p}${this.allocateParam(param)}${s}`;
  }

  orIsNullCheck(column, not) {
    return `${this.shouldAddOrIsNull(not) ? ` OR ${column} IS NULL` : ''}`;
  }

  shouldAddOrIsNull(not) {
    return not ? !this.valuesContainNull() : this.valuesContainNull();
  }

  allocateParam(param) {
    return this.query.paramAllocator.allocateParam(param);
  }

  equalsWhere(column) {
    if (this.isArrayValues()) {
      return this.inWhere(column);
    }

    if (this.valuesContainNull()) {
      return this.notSetWhere(column);
    }

    return `${column} = ${this.firstParameter()}${this.orIsNullCheck(column, false)}`;
  }

  inPlaceholders() {
    return `(${join(', ', this.filterParams().map(p => this.allocateCastParam(p)))})`;
  }

  inWhere(column) {
    return `${column} IN ${this.inPlaceholders()}${this.orIsNullCheck(column, false)}`;
  }

  notEqualsWhere(column) {
    if (this.isArrayValues()) {
      return this.notInWhere(column);
    }

    if (this.valuesContainNull()) {
      return this.setWhere(column);
    }

    return `${column} <> ${this.firstParameter()}${this.orIsNullCheck(column, true)}`;
  }

  notInWhere(column) {
    return `${column} NOT IN ${this.inPlaceholders()}${this.orIsNullCheck(column, true)}`;
  }

  setWhere(column) {
    return `${column} IS NOT NULL`;
  }

  notSetWhere(column) {
    return `${column} IS NULL`;
  }

  gtWhere(column) {
    return `${column} > ${this.firstParameter()}`;
  }

  gteWhere(column) {
    return `${column} >= ${this.firstParameter()}`;
  }

  ltWhere(column) {
    return `${column} < ${this.firstParameter()}`;
  }

  lteWhere(column) {
    return `${column} <= ${this.firstParameter()}`;
  }

  expressionEqualsWhere(column) {
    return `${column} = ${this.values[0]}`;
  }

  inDateRangeWhere(column) {
    const [from, to] = this.allocateTimestampParams();
    return this.query.timeRangeFilter(column, from, to);
  }

  notInDateRangeWhere(column) {
    const [from, to] = this.allocateTimestampParams();
    return this.query.timeNotInRangeFilter(column, from, to);
  }

  onTheDateWhere(column) {
    const [from, to] = this.allocateTimestampParams();
    return this.query.timeRangeFilter(column, from, to);
  }

  beforeDateWhere(column) {
    const [before] = this.allocateTimestampParams();
    return this.query.beforeDateFilter(column, before);
  }

  beforeOrOnDateWhere(column) {
    const [before] = this.allocateTimestampParams();
    return this.query.beforeOrOnDateFilter(column, before);
  }

  afterDateWhere(column) {
    const [after] = this.allocateTimestampParams();
    return this.query.afterDateFilter(column, after);
  }

  afterOrOnDateWhere(column) {
    const [after] = this.allocateTimestampParams();
    return this.query.afterOrOnDateFilter(column, after);
  }

  formatFromDate(date) {
    if (date && date.match(dateTimeLocalMsRegex)) {
      return date;
    }
    if (date && date.match(dateRegex)) {
      return `${date}T00:00:00.000`;
    }
    if (!date) {
      return moment.tz(date, this.query.timezone).format('YYYY-MM-DDT00:00:00.000');
    }
    return moment.tz(date, this.query.timezone).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }

  inDbTimeZoneDateFrom(date) {
    if (date && (date === FROM_PARTITION_RANGE || date === TO_PARTITION_RANGE)) {
      return date;
    }
    return this.query.inDbTimeZone(this.formatFromDate(date));
  }

  formatToDate(date) {
    if (date && date.match(dateTimeLocalMsRegex)) {
      return date;
    }
    if (date && date.match(dateRegex)) {
      return `${date}T23:59:59.999`;
    }
    if (!date) {
      return moment.tz(date, this.query.timezone).format('YYYY-MM-DDT23:59:59.999');
    }
    return moment.tz(date, this.query.timezone).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }

  inDbTimeZoneDateTo(date) {
    if (date && (date === FROM_PARTITION_RANGE || date === TO_PARTITION_RANGE)) {
      return date;
    }
    return this.query.inDbTimeZone(this.formatToDate(date));
  }

  formattedDateRange() {
    return [this.formatFromDate(this.values[0]), this.formatToDate(this.values[1])];
  }

  unescapedAliasName() {
    return this.query.aliasName(this.measure || this.dimension);
  }
}
