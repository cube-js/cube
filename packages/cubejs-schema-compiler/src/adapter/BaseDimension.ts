import type { BaseQuery } from './BaseQuery';
import type { DimensionDefinition, SegmentDefinition } from '../compiler/CubeEvaluator';
import { CubeSymbols } from "../compiler/CubeSymbols";

export class BaseDimension {
  public readonly expression: any;

  public readonly expressionCubeName: any;

  public readonly expressionName: string | undefined;

  public readonly isMemberExpression: boolean = false;

  public readonly joinHint: Array<string> = [];

  public constructor(
    protected readonly query: BaseQuery,
    public readonly dimension: any
  ) {
    if (dimension && dimension.expression) {
      this.expression = dimension.expression;
      this.expressionCubeName = dimension.cubeName;
      // In case of SQL push down expressionName doesn't contain cube name. It's just a column name.
      this.expressionName = dimension.expressionName || `${dimension.cubeName}.${dimension.name}`;
      this.isMemberExpression = !!dimension.definition;
    } else {
      // TODO move this `as` to static types
      const dimensionPath = dimension as string | null;
      if (dimensionPath !== null) {
        const { path, joinHint } = CubeSymbols.joinHintFromPath(dimensionPath);
        this.dimension = path;
        this.joinHint = joinHint;
      }
    }
  }

  public selectColumns(): string[] | null {
    return [`${this.dimensionSql()} ${this.aliasName()}`];
  }

  public hasNoRemapping() {
    return this.dimensionSql() === this.aliasName();
  }

  public cumulativeSelectColumns() {
    return [`${this.aliasName()}`];
  }

  public dimensionSql() {
    if (this.expression) {
      return this.convertTzForRawTimeDimensionIfNeeded(() => this.query.evaluateSymbolSql(this.expressionCubeName, this.expressionName, this.definition(), 'dimension'));
    }
    if (this.query.cubeEvaluator.isSegment(this.dimension)) {
      return this.query.wrapSegmentForDimensionSelect(this.query.dimensionSql(this));
    }
    return this.convertTzForRawTimeDimensionIfNeeded(() => this.query.dimensionSql(this));
  }

  // We need this for dimensions however we don't for filters for performance reasons
  public convertTzForRawTimeDimensionIfNeeded(sql) {
    if (this.query.options.convertTzForRawTimeDimension) {
      return this.query.evaluateSymbolSqlWithContext(sql, {
        convertTzForRawTimeDimension: true
      });
    } else {
      return sql();
    }
  }

  public sqlDefinition() {
    return this.dimensionDefinition().sql;
  }

  public getMembers() {
    return [this];
  }

  public isMultiStage() {
    if (this.expression) { // TODO
      return false;
    }
    return this.definition().multiStage;
  }

  public cube() {
    if (this.expression) {
      return this.query.cubeEvaluator.cubeFromPath(this.expressionCubeName);
    }
    return this.query.cubeEvaluator.cubeFromPath(this.dimension);
  }

  public dimensionDefinition(): DimensionDefinition | SegmentDefinition {
    if (this.query.cubeEvaluator.isSegment(this.dimension)) {
      return this.query.cubeEvaluator.segmentByPath(this.dimension);
    }

    return this.query.cubeEvaluator.dimensionByPath(this.dimension);
  }

  public definition(): DimensionDefinition | SegmentDefinition {
    if (this.expression) {
      return {
        sql: this.expression,
        // TODO use actual dimension type even though it isn't used right now
        type: 'number'
      } as DimensionDefinition;
    }

    return this.dimensionDefinition();
  }

  public aliasName(): string | null {
    // Require should be here because of cycle depend
    return this.query.escapeColumnName(this.unescapedAliasName());
  }

  public unescapedAliasName(): string {
    if (this.expression && this.expressionName) {
      return this.query.aliasName(this.expressionName);
    }

    return this.query.aliasName(this.dimension);
  }

  public dateFieldType() {
    return this.dimensionDefinition().fieldType;
  }

  public path() {
    if (this.expression) {
      return null;
    }

    if (this.query.cubeEvaluator.isSegment(this.dimension)) {
      return this.query.cubeEvaluator.parsePath('segments', this.dimension);
    }

    return this.query.cubeEvaluator.parsePath('dimensions', this.dimension);
  }

  public expressionPath() {
    if (this.expression) {
      return `expr:${this.expression.expressionName}`;
    }
    return this.query.cubeEvaluator.pathFromArray(this.path());
  }
}
