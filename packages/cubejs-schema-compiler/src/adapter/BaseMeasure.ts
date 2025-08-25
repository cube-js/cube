import { UserError } from '../compiler/UserError';
import type { BaseQuery } from './BaseQuery';
import { MeasureDefinition } from '../compiler/CubeEvaluator';
import { CubeSymbols } from '../compiler/CubeSymbols';

export class BaseMeasure {
  public readonly expression: any;

  public readonly expressionCubeName: any;

  public readonly expressionName: any;

  public readonly isMemberExpression: boolean = false;

  protected readonly patchedMeasure: MeasureDefinition | null = null;

  public readonly joinHint: Array<string> = [];

  protected preparePatchedMeasure(sourceMeasure: string, newMeasureType: string | null, addFilters: Array<{sql: Function}>): MeasureDefinition {
    const source = this.query.cubeEvaluator.measureByPath(sourceMeasure);

    let resultMeasureType = source.type;
    if (newMeasureType !== null) {
      switch (source.type) {
        case 'sum':
        case 'avg':
        case 'min':
        case 'max':
          switch (newMeasureType) {
            case 'sum':
            case 'avg':
            case 'min':
            case 'max':
            case 'count_distinct':
            case 'count_distinct_approx':
              // Can change from avg/... to count_distinct
              // Latter does not care what input value is
              // ok, do nothing
              break;
            default:
              throw new UserError(
                `Unsupported measure type replacement for ${sourceMeasure}: ${source.type} => ${newMeasureType}`
              );
          }
          break;
        case 'count_distinct':
        case 'count_distinct_approx':
          switch (newMeasureType) {
            case 'count_distinct':
            case 'count_distinct_approx':
              // ok, do nothing
              break;
            default:
              // Can not change from count_distinct to avg/...
              // Latter do care what input value is, and original measure can be defined on strings
              throw new UserError(
                `Unsupported measure type replacement for ${sourceMeasure}: ${source.type} => ${newMeasureType}`
              );
          }
          break;
        default:
          // Can not change from string, time, boolean, number
          // Aggregation is already included in SQL, it's hard to patch that
          // Can not change from count
          // There's no SQL at all
          throw new UserError(
            `Unsupported measure type replacement for ${sourceMeasure}: ${source.type} => ${newMeasureType}`
          );
      }

      resultMeasureType = newMeasureType;
    }

    const resultFilters = source.filters ?? [];

    if (addFilters.length > 0) {
      switch (resultMeasureType) {
        case 'sum':
        case 'avg':
        case 'min':
        case 'max':
        case 'count':
        case 'count_distinct':
        case 'count_distinct_approx':
          // ok, do nothing
          break;
        default:
          // Can not add filters to string, time, boolean, number
          // Aggregation is already included in SQL, it's hard to patch that
          throw new UserError(
            `Unsupported additional filters for measure ${sourceMeasure} type ${source.type}`
          );
      }

      resultFilters.push(...addFilters);
    }

    const patchedFrom = this.query.cubeEvaluator.parsePath('measures', sourceMeasure);

    return {
      ...source,
      type: resultMeasureType,
      filters: resultFilters,
      patchedFrom: {
        cubeName: patchedFrom[0],
        name: patchedFrom[1],
      },
    };
  }

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

      if (measure.expression.type === 'PatchMeasure') {
        this.patchedMeasure = this.preparePatchedMeasure(
          measure.expression.sourceMeasure,
          measure.expression.replaceAggregationType,
          measure.expression.addFilters,
        );
      }
    } else {
      // TODO move this `as` to static types
      const measurePath = measure as string;
      const { path, joinHint } = CubeSymbols.joinHintFromPath(measurePath);
      this.measure = path;
      this.joinHint = joinHint;
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
      return this.convertTzForRawTimeDimensionIfNeeded(() => this.query.evaluateSymbolSql(this.expressionCubeName, this.expressionName, this.definition(), 'measure'));
    }
    return this.query.measureSql(this);
  }

  // We need this for measures however we don't for filters for performance reasons
  public convertTzForRawTimeDimensionIfNeeded(sql) {
    if (this.query.options.convertTzForRawTimeDimension) {
      return this.query.evaluateSymbolSqlWithContext(sql, {
        convertTzForRawTimeDimension: true
      });
    } else {
      return sql();
    }
  }

  public cube() {
    if (this.expression) {
      return this.query.cubeEvaluator.cubeFromPath(this.expressionCubeName);
    }
    return this.query.cubeEvaluator.cubeFromPath(this.measure);
  }

  public measureDefinition() {
    if (this.patchedMeasure) {
      return this.patchedMeasure;
    }
    return this.query.cubeEvaluator.measureByPath(this.measure);
  }

  public definition(): any {
    if (this.patchedMeasure) {
      return this.patchedMeasure;
    }
    if (this.expression) {
      return {
        sql: this.expression,
        // TODO use actual measure type even though it isn't used right now
        type: 'number'
      };
    }
    return this.measureDefinition();
  }

  public aliasName(): string {
    return this.query.escapeColumnName(this.unescapedAliasName());
  }

  public unescapedAliasName(): string {
    if (this.expression) {
      return this.query.aliasName(this.expressionName);
    }
    return this.query.aliasName(this.measure);
  }

  public isCumulative(): boolean {
    if (this.expression) { // TODO
      return false;
    }
    return BaseMeasure.isCumulative(this.measureDefinition());
  }

  public isMultiStage(): boolean {
    if (this.expression) { // TODO
      return false;
    }
    return this.definition().multiStage;
  }

  public isAdditive(): boolean {
    if (this.expression) { // TODO
      return false;
    }
    const definition = this.measureDefinition();
    if (definition.multiStage) {
      return false;
    }
    return definition.type === 'sum' || definition.type === 'count' || definition.type === 'countDistinctApprox' ||
      definition.type === 'min' || definition.type === 'max';
  }

  public static isCumulative(definition): boolean {
    return definition.type === 'runningTotal' || !!definition.rollingWindow;
  }

  public rollingWindowDefinition() {
    if (this.measureDefinition().type === 'runningTotal') {
      throw new UserError('runningTotal rollups aren\'t supported. Please consider replacing runningTotal measure with rollingWindow.');
    }
    const { type } = this.measureDefinition().rollingWindow;
    if (type && type !== 'fixed') {
      throw new UserError(`Only fixed rolling windows are supported by Cube Store but got '${type}' rolling window`);
    }
    return this.measureDefinition().rollingWindow;
  }

  public dateJoinCondition() {
    const definition = this.measureDefinition();
    if (definition.type === 'runningTotal') {
      return this.query.runningTotalDateJoinCondition();
    }
    const { rollingWindow } = definition;
    if (rollingWindow.type === 'to_date') {
      return this.query.rollingWindowToDateJoinCondition(rollingWindow.granularity);
    }
    // TODO deprecated
    if (rollingWindow.type === 'year_to_date' || rollingWindow.type === 'quarter_to_date' || rollingWindow.type === 'month_to_date') {
      return this.query.rollingWindowToDateJoinCondition(rollingWindow.type.replace('_to_date', ''));
    }
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

  public minGranularity(granularityA: string | undefined, granularityB: string | undefined) {
    return this.query.minGranularity(granularityA, granularityB);
  }

  public granularityFromInterval(interval: string): string | undefined {
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

  public shouldUngroupForCumulative(): boolean {
    return this.measureDefinition().rollingWindow && !this.isAdditive();
  }

  public sqlDefinition() {
    return this.measureDefinition().sql;
  }

  public path(): string[] | null {
    if (this.expression) {
      return null;
    }
    return this.query.cubeEvaluator.parsePath('measures', this.measure);
  }

  public expressionPath(): string {
    if (this.expression) {
      return `expr:${this.expressionName}`;
    }

    const path = this.path();
    if (path === null) {
      // Sanity check, this should not actually happen because we checked this.expression earlier
      throw new Error('Unexpected null path');
    }
    return this.query.cubeEvaluator.pathFromArray(path);
  }
}
