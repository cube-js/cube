import type { BaseQuery } from './BaseQuery';

export class BaseSegment {
  public readonly expression: any;

  public readonly expressionCubeName: any;

  public readonly expressionName: any;

  public readonly isMemberExpression: boolean = false;

  public readonly joinHint: Array<string> = [];

  // The path the member was requested by, when it goes through a join alias
  public readonly aliasPath: string | undefined;

  public constructor(
    protected readonly query: BaseQuery,
    public readonly segment: string | any
  ) {
    if (segment.expression) {
      this.expression = segment.expression;
      this.expressionCubeName = segment.cubeName;
      // In case of SQL push down expressionName doesn't contain cube name. It's just a column name.
      this.expressionName = segment.expressionName || `${segment.cubeName}.${segment.name}`;
      this.isMemberExpression = !!segment.definition;
    } else {
      // TODO move this `as` to static types
      const segmentPath = segment as string;
      const { path, joinHint, aliasPath } = this.query.memberPathForModel(segmentPath);
      this.segment = path;
      this.joinHint = joinHint;
      this.aliasPath = aliasPath;
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

  public isMultiStage() {
    if (this.expression) { // TODO
      return false;
    }
    return this.definition().multiStage;
  }

  public definition(): any {
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

  public path(): string[] | null {
    if (this.expression) {
      return null;
    }
    return this.query.cubeEvaluator.parsePath('segments', this.segment);
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
