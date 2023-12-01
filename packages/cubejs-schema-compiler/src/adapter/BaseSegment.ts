import type { BaseQuery } from './BaseQuery';

export class BaseSegment {
  public constructor(
    public readonly query: BaseQuery,
    public readonly segment: string
  ) {}

  public filterToWhere() {
    return this.query.segmentSql(this);
  }

  public filterParams() {
    return [];
  }

  public segmentDefinition() {
    return this.query.cubeEvaluator.segmentByPath(this.segment);
  }

  public definition() {
    return this.segmentDefinition();
  }

  public getMembers() {
    return [this];
  }

  public cube() {
    return this.query.cubeEvaluator.cubeFromPath(this.segment);
  }

  public sqlDefinition() {
    return this.segmentDefinition().sql;
  }

  public path() {
    return this.query.cubeEvaluator.parsePath('segments', this.segment);
  }

  public expressionPath() {
    // TODO expression support
    // if (this.expression) {
    //   return `expr:${this.expression.expressionName}`;
    // }
    return this.query.cubeEvaluator.pathFromArray(this.path());
  }
}
