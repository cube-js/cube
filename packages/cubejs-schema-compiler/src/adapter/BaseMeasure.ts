import { UserError } from '../compiler/UserError';
import type { BaseQuery } from './BaseQuery';

export class BaseMeasure {
  public readonly expression: any;

  public readonly expressionCubeName: any;

  public readonly expressionName: any;

  public readonly isMemberExpression: boolean = false;

  public constructor(
    protected readonly query: BaseQuery,
    public readonly measure: any
  ) {
    if (measure.expression) {
      this.expression = measure.expression;
      this.expressionCubeName = measure.cubeName;
      // In case of SQL push down expressionName doesn't contain cube name. It's just a column name.
      this.expressionName = measure.expressionName || `${measure.cubeName}.${measure.name}`;
      this.isMemberExpression = !!measure.definition;
    }
  }

  public getMembers() {
    return [this];
  }

  public selectColumns() {
    return [`${this.measureSql()} ${this.aliasName()}`];
  }

  public hasNoRemapping() {
    return this.measureSql() === this.aliasName();
  }

  public cumulativeSelectColumns() {
    return [`${this.cumulativeMeasureSql()} ${this.aliasName()}`];
  }

  public cumulativeMeasureSql() {
    return this.query.evaluateSymbolSqlWithContext(
      () => this.measureSql(),
      {
        ungroupedAliasesForCumulative: { [this.measure]: this.aliasName() }
      }
    );
  }

  public measureSql() {
    if (this.expression) {
      return this.query.evaluateSymbolSql(this.expressionCubeName, this.expressionName, this.definition(), 'measure');
    }
    return this.query.measureSql(this);
  }

  public cube() {
    if (this.expression) {
      return this.query.cubeEvaluator.cubeFromPath(this.expressionCubeName);
    }
    return this.query.cubeEvaluator.cubeFromPath(this.measure);
  }

  public measureDefinition() {
    return this.query.cubeEvaluator.measureByPath(this.measure);
  }

  public definition(): any {
    if (this.expression) {
      return {
        sql: this.expression,
        // TODO use actual measure type even though it isn't used right now
        type: 'number'
      };
    }
    return this.measureDefinition();
  }

  public aliasName() {
    return this.query.escapeColumnName(this.unescapedAliasName());
  }

  public unescapedAliasName() {
    if (this.expression) {
      return this.query.aliasName(this.expressionName);
    }
    return this.query.aliasName(this.measure);
  }

  public isCumulative() {
    if (this.expression) { // TODO
      return false;
    }
    return BaseMeasure.isCumulative(this.measureDefinition());
  }

  public isPostAggregate() {
    if (this.expression) { // TODO
      return false;
    }
    return this.definition().postAggregate;
  }

  public isAdditive() {
    if (this.expression) { // TODO
      return false;
    }
    const definition = this.measureDefinition();
    return definition.type === 'sum' || definition.type === 'count' || definition.type === 'countDistinctApprox' ||
      definition.type === 'min' || definition.type === 'max';
  }

  public static isCumulative(definition) {
    return definition.type === 'runningTotal' || !!definition.rollingWindow;
  }

  public rollingWindowDefinition() {
    if (this.measureDefinition().type === 'runningTotal') {
      throw new UserError('runningTotal rollups aren\'t supported. Please consider replacing runningTotal measure with rollingWindow.');
    }
    return this.measureDefinition().rollingWindow;
  }

  public dateJoinCondition() {
    if (this.measureDefinition().type === 'runningTotal') {
      return this.query.runningTotalDateJoinCondition();
    }
    const { rollingWindow } = this.measureDefinition();
    if (rollingWindow) {
      return this.query.rollingWindowDateJoinCondition(
        rollingWindow.trailing, rollingWindow.leading, rollingWindow.offset
      );
    }
    return null;
  }

  public windowGranularity() {
    const { rollingWindow } = this.measureDefinition();
    if (rollingWindow) {
      return this.minGranularity(
        this.granularityFromInterval(rollingWindow.leading),
        this.granularityFromInterval(rollingWindow.trailing)
      );
    }
    return undefined;
  }

  public minGranularity(granularityA, granularityB) {
    return this.query.minGranularity(granularityA, granularityB);
  }

  public granularityFromInterval(interval) {
    if (!interval) {
      return undefined;
    }
    if (interval.match(/day/)) {
      return 'day';
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

  public shouldUngroupForCumulative() {
    return this.measureDefinition().rollingWindow && !this.isAdditive();
  }

  public sqlDefinition() {
    return this.measureDefinition().sql;
  }

  public path() {
    if (this.expression) {
      return null;
    }
    return this.query.cubeEvaluator.parsePath('measures', this.measure);
  }

  public expressionPath() {
    if (this.expression) {
      return `expr:${this.expression.expressionName}`;
    }
    return this.query.cubeEvaluator.pathFromArray(this.path());
  }
}
