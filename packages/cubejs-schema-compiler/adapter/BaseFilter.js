const inlection = require('inflection');
const momentRange = require('moment-range');
const moment = momentRange.extendMoment(require('moment-timezone'));
const {
  repeat, join, map, contains
} = require('ramda');

const BaseDimension = require('./BaseDimension');

const DATE_OPERATORS = ['in_date_range', 'not_in_date_range', 'on_the_date', 'before_date', 'after_date'];
const dateTimeLocalMsRegex = /^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d$/;
const dateRegex = /^\d\d\d\d-\d\d-\d\d$/;

class BaseFilter extends BaseDimension {
  constructor(query, filter) {
    super(query, filter.dimension);
    this.measure = filter.measure;
    this.query = query;
    this.operator = filter.operator;
    this.values = filter.values;
  }

  filterToWhere() {
    if (this.operator === 'measure_filter' || this.operator === 'measureFilter') {
      return this.measureFilterToWhere();
    }

    return this.conditionSql(this.measure ? this.query.measureSql(this) : this.query.dimensionSql(this));
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
    const operatorMethod = `${inlection.camelize(this.operator).replace(
      /[A-Z]/,
      (c) => (c != null ? c : '').toLowerCase()
    )}Where`;

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
    return this.measure ?
      this.query.cubeEvaluator.parsePath('measures', this.measure) :
      this.query.cubeEvaluator.parsePath('dimensions', this.dimension);
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
    return this.operator === 'contains' || this.operator === 'not_contains';
  }

  filterParams() {
    if (contains(this.operator, DATE_OPERATORS)) {
      return [this.inDbTimeZoneDateFrom(this.values[0]), this.inDbTimeZoneDateTo(this.values[1])];
    }
    if (this.operator === 'set' || this.operator === 'not_set' || this.operator === 'expressionEquals') {
      return [];
    }
    const params = this.valuesArray().filter(v => v != null);

    if (this.isWildcardOperator()) {
      return map(this.escapeWildcardChars, params);
    }

    return params;
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
    if (!params[0]) {
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
    return this.filterParams().map(p => this.allocateTimestampParam(p));
  }

  allParamsRepeat(basePart) {
    return this.filterParams().map(p => this.query.paramAllocator.allocateParamsForQuestionString(basePart, [p]));
  }

  isArrayValues() {
    return Array.isArray(this.values) && this.values.length > 1;
  }

  containsWhere(column) {
    return this.likeOr(column);
  }

  notContainsWhere(column) {
    return this.likeOr(column, true);
  }

  likeOr(column, not) {
    return `${join(not ? ' AND ' : ' OR ', this.filterParams().map(p => this.likeIgnoreCase(column, not, p)))}${this.orIsNullCheck(column, not)}`;
  }

  orIsNullCheck(column, not) {
    return `${this.shouldAddOrIsNull(not) ? ` OR ${column} IS NULL` : ''}`;
  }

  shouldAddOrIsNull(not) {
    return not ? !this.valuesContainNull() : this.valuesContainNull();
  }

  likeIgnoreCase(column, not, param) {
    return `${column}${not ? ' NOT' : ''} ILIKE '%' || ${this.allocateParam(param)} || '%'`;
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

  afterDateWhere(column) {
    const [after] = this.allocateTimestampParams();
    return this.query.afterDateFilter(column, after);
  }

  formatFromDate(date) {
    if (date && date.match(dateTimeLocalMsRegex)) {
      return date;
    }
    if (date && date.match(dateRegex)) {
      return `${date}T00:00:00.000`;
    }
    if (!date) {
      return moment.tz(date, this.query.timezone).format('YYYY-MM-DD 00:00:00');
    }
    return moment.tz(date, this.query.timezone).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }

  inDbTimeZoneDateFrom(date) {
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
    return this.query.inDbTimeZone(this.formatToDate(date));
  }

  unescapedAliasName() {
    return this.query.aliasName(this.measure || this.dimension);
  }
}

module.exports = BaseFilter;
