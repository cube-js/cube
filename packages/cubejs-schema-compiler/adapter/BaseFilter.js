const inlection = require('inflection');
const momentRange = require('moment-range');
const moment = momentRange.extendMoment(require('moment-timezone'));
const { repeat, join, map, contains } = require('ramda');

const BaseDimension = require('./BaseDimension');

const DATE_OPERATORS = ['in_date_range', 'not_in_date_range', 'on_the_date', 'before_date', 'after_date'];

class BaseFilter extends BaseDimension {
  constructor(query, filter) {
    super(query, filter.dimension);
    this.measure = filter.measure;
    this.query = query;
    this.operator = filter.operator;
    this.values = filter.values;
  }

  filterToWhere() {
    if (this.operator === 'measure_filter') {
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
    const operatorMethod = `${inlection.camelize(this.operator).replace(/[A-Z]/, (c) =>
      (c != null ? c : '').toLowerCase()
    )}Where`;
    const sql = this[operatorMethod](columnSql);
    return this.query.paramAllocator.allocateParamsForQuestionString(sql, this.filterParams());
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
    return typeof param === 'string'
      ? param.replace(/([_%])/gi, '\\$1')
      : param;
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
    const params = Array.isArray(this.values) ? this.values : [this.values];

    if (this.isWildcardOperator()) {
      return map(this.escapeWildcardChars, params);
    }

    return params;
  }

  castParameter() {
    return '?';
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
    const basePart = this.likeIgnoreCase(column, not);
    const nullCheck = `${not ? ` OR ${column} IS NULL` : ''}`;
    return `${join(not ? ' AND ' : ' OR ', repeat(basePart, this.values.length))}${nullCheck}`;
  }

  likeIgnoreCase(column, not) {
    return `${column}${not ? ' NOT' : ''} ILIKE '%' || ? || '%'`;
  }

  equalsWhere(column) {
    if (this.isArrayValues()) {
      return this.inWhere(column);
    }

    return `${column} = ${this.castParameter()}`;
  }

  inPlaceholders() {
    return `(${join(', ', repeat(this.castParameter(), this.values.length || 1))})`;
  }

  inWhere(column) {
    return `${column} IN ${this.inPlaceholders()}`;
  }

  notEqualsWhere(column) {
    if (this.isArrayValues()) {
      return this.notInWhere(column);
    }

    return `${column} <> ${this.castParameter()}`;
  }

  notInWhere(column) {
    return `${column} NOT IN ${this.inPlaceholders()}`;
  }

  setWhere(column) {
    return `${column} IS NOT NULL`;
  }

  notSetWhere(column) {
    return `${column} IS NULL`;
  }

  gtWhere(column) {
    return `${column} > ${this.castParameter()}`;
  }

  gteWhere(column) {
    return `${column} >= ${this.castParameter()}`;
  }

  ltWhere(column) {
    return `${column} < ${this.castParameter()}`;
  }

  lteWhere(column) {
    return `${column} <= ${this.castParameter()}`;
  }

  expressionEqualsWhere(column) {
    return `${column} = ${this.values[0]}`;
  }

  inDateRangeWhere(column) {
    return this.query.timeRangeFilter(column, this.query.timeStampParam(this), this.query.timeStampParam(this));
  }

  notInDateRangeWhere(column) {
    return this.query.timeNotInRangeFilter(column, this.query.timeStampParam(this), this.query.timeStampParam(this));
  }

  onTheDateWhere(column) {
    return this.query.timeRangeFilter(column, this.query.timeStampParam(this), this.query.timeStampParam(this));
  }

  beforeDateWhere(column) {
    return this.query.beforeDateFilter(column, this.query.timeStampParam(this));
  }

  afterDateWhere(column) {
    return this.query.afterDateFilter(column, this.query.timeStampParam(this));
  }

  formatFromDate(date) {
    return moment.tz(date, this.query.timezone).format('YYYY-MM-DD 00:00:00');
  }

  inDbTimeZoneDateFrom(date) {
    return this.query.inDbTimeZone(this.formatFromDate(date));
  }

  formatToDate(date) {
    return moment.tz(date, this.query.timezone).format('YYYY-MM-DD 23:59:59');
  }

  inDbTimeZoneDateTo(date) {
    return this.query.inDbTimeZone(this.formatToDate(date));
  }

  unescapedAliasName() {
    return this.query.aliasName(this.measure || this.dimension);
  }
}

module.exports = BaseFilter;
