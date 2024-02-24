import type { BaseQuery } from './BaseQuery';

export class BaseSegment {
  public readonly expression: any;

  public readonly expressionCubeName: any;

  public readonly expressionName: any;

  public readonly isMemberExpression: boolean = false;

  public constructor(
    protected readonly query: BaseQuery,
    public readonly segment: string | any
  ) {
    if (segment.expression) {
      this.expression = segment.expression;
      this.expressionCubeName = segment.cubeName;
      this.expressionName = segment.expressionName || `${segment.cubeName}.${segment.name}`;
      this.isMemberExpression = !!segment.definition;
    }
  }

  public filterToWhere() {
    return this.segmentSql();
  }

  public segmentSql() {
    if (this.expression) {
      return this.query.evaluateSymbolSql(this.expressionCubeName, this.expressionName, this.definition(), 'segment');
    }
    return this.query.segmentSql(this);
  }

  public filterParams() {
    return [];
  }

  public segmentDefinition() {
    return this.query.cubeEvaluator.segmentByPath(this.segment);
  }

  public definition() {
    if (this.expression) {
      return {
        sql: this.expression
      };
    }
    return this.segmentDefinition();
  }

  public getMembers() {
    return [this];
  }

  public cube() {
    if (this.expression) {
      return this.query.cubeEvaluator.cubeFromPath(this.expressionCubeName);
    }
    return this.query.cubeEvaluator.cubeFromPath(this.segment);
  }

  public sqlDefinition() {
    return this.segmentDefinition().sql;
  }

  public path() {
    if (this.expression) {
      return null;
    }
    return this.query.cubeEvaluator.parsePath('segments', this.segment);
  }

  public expressionPath() {
    if (this.expression) {
      return `expr:${this.expression.expressionName}`;
    }
    return this.query.cubeEvaluator.pathFromArray(this.path());
  }
}
