class BaseMeasure {
  constructor(query, measure) {
    this.query = query;
    if (measure.expression) {
      this.expression = measure.expression;
      this.expressionCubeName = measure.cubeName;
      this.expressionName = `${measure.cubeName}.${measure.name}`;
    }
    this.measure = measure;
  }

  selectColumns() {
    return [`${this.measureSql()} ${this.aliasName()}`];
  }

  cumulativeSelectColumns() {
    return [`${this.cumulativeMeasureSql()} ${this.aliasName()}`];
  }

  cumulativeMeasureSql() {
    return this.query.evaluateSymbolSqlWithContext(
      () => this.measureSql(),
      {
        ungroupedAliasesForCumulative: { [this.measure]: this.aliasName() }
      }
    );
  }

  measureSql() {
    if (this.expression) {
      return this.query.evaluateSql(this.expressionCubeName, this.expression);
    }
    return this.query.measureSql(this);
  }

  cube() {
    if (this.expression) {
      return this.query.cubeEvaluator.cubeFromPath(this.expressionCubeName);
    }
    return this.query.cubeEvaluator.cubeFromPath(this.measure);
  }

  measureDefinition() {
    return this.query.cubeEvaluator.measureByPath(this.measure);
  }

  definition() {
    if (this.expression) {
      return {
        sql: this.expression,
        type: 'number'
      };
    }
    return this.measureDefinition();
  }

  aliasName() {
    if (this.expression) {
      return this.query.escapeColumnName(this.query.aliasName(this.expressionName));
    }
    return this.query.escapeColumnName(this.query.aliasName(this.measure));
  }

  isCumulative() {
    if (this.expression) { // TODO
      return false;
    }
    return BaseMeasure.isCumulative(this.measureDefinition());
  }

  isAdditive() {
    if (this.expression) { // TODO
      return false;
    }
    const definition = this.measureDefinition();
    return definition.type === 'sum' || definition.type === 'count' || definition.type === 'countDistinctApprox' ||
      definition.type === 'min' || definition.type === 'max';
  }

  static isCumulative(definition) {
    return definition.type === 'runningTotal' || !!definition.rollingWindow;
  }

  dateJoinCondition() {
    if (this.measureDefinition().type === 'runningTotal') {
      return this.query.runningTotalDateJoinCondition();
    }
    const rollingWindow = this.measureDefinition().rollingWindow;
    if (rollingWindow) {
      return this.query.rollingWindowDateJoinCondition(
        rollingWindow.trailing, rollingWindow.leading, rollingWindow.offset
      );
    }
    return null;
  }

  windowGranularity() {
    const rollingWindow = this.measureDefinition().rollingWindow;
    if (rollingWindow) {
      return this.minGranularity(
        this.granularityFromInterval(rollingWindow.leading),
        this.granularityFromInterval(rollingWindow.trailing)
      );
    }
    return undefined;
  }

  minGranularity(granularityA, granularityB) {
    return this.query.minGranularity(granularityA, granularityB);
  }

  granularityFromInterval(interval) {
    if (!interval) {
      return undefined;
    }
    if (interval.match(/day/)) {
      return 'date';
    } else if (interval.match(/month/)) {
      return 'month';
    } else if (interval.match(/year/)) {
      return 'year';
    } else if (interval.match(/week/)) {
      return 'week';
    } else if (interval.match(/hour/)) {
      return 'hour';
    }
    return undefined;
  }

  shouldUngroupForCumulative() {
    return this.measureDefinition().rollingWindow && !this.isAdditive();
  }

  sqlDefinition() {
    return this.measureDefinition().sql;
  }

  path() {
    if (this.expression) {
      return null;
    }
    return this.query.cubeEvaluator.parsePath('measures', this.measure);
  }
}

module.exports = BaseMeasure;