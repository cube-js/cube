import type { BaseQuery } from './BaseQuery';

export class BaseDimension {
  public readonly expression: any;

  public readonly expressionCubeName: any;

  public readonly expressionName: string | undefined;

  public readonly isMemberExpression: boolean = false;

  public constructor(
    protected readonly query: BaseQuery,
    public readonly dimension: any
  ) {
    if (dimension && dimension.expression) {
      this.expression = dimension.expression;
      this.expressionCubeName = dimension.cubeName;
      this.expressionName = dimension.expressionName || `${dimension.cubeName}.${dimension.name}`;
      this.isMemberExpression = !!dimension.definition;
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

  public cube() {
    if (this.expression) {
      return this.query.cubeEvaluator.cubeFromPath(this.expressionCubeName);
    }
    return this.query.cubeEvaluator.cubeFromPath(this.dimension);
  }

  public dimensionDefinition() {
    if (this.query.cubeEvaluator.isSegment(this.dimension)) {
      return this.query.cubeEvaluator.segmentByPath(this.dimension);
    }
    return this.query.cubeEvaluator.dimensionByPath(this.dimension);
  }

  public definition() {
    if (this.expression) {
      return {
        sql: this.expression,
        // TODO use actual dimension type even though it isn't used right now
        type: 'number'
      };
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
