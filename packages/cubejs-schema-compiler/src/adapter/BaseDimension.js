export class BaseDimension {
  constructor(query, dimension) {
    this.query = query;
    if (dimension && dimension.expression) {
      this.expression = dimension.expression;
      this.expressionCubeName = dimension.cubeName;
      this.expressionName = dimension.expressionName || `${dimension.cubeName}.${dimension.name}`;
      this.isMemberExpression = !!dimension.definition;
    }
    this.dimension = dimension;
  }

  selectColumns() {
    return [`${this.dimensionSql()} ${this.aliasName()}`];
  }

  hasNoRemapping() {
    return this.dimensionSql() === this.aliasName();
  }

  cumulativeSelectColumns() {
    return [`${this.aliasName()}`];
  }

  dimensionSql() {
    if (this.expression) {
      return this.query.evaluateSymbolSql(this.expressionCubeName, this.expressionName, this.definition(), 'dimension');
    }
    if (this.query.cubeEvaluator.isSegment(this.dimension)) {
      return this.query.wrapSegmentForDimensionSelect(this.query.dimensionSql(this));
    }
    return this.query.dimensionSql(this);
  }

  sqlDefinition() {
    return this.dimensionDefinition().sql;
  }

  getMembers() {
    return [this];
  }

  cube() {
    if (this.expression) {
      return this.query.cubeEvaluator.cubeFromPath(this.expressionCubeName);
    }
    return this.query.cubeEvaluator.cubeFromPath(this.dimension);
  }

  dimensionDefinition() {
    if (this.query.cubeEvaluator.isSegment(this.dimension)) {
      return this.query.cubeEvaluator.segmentByPath(this.dimension);
    }
    return this.query.cubeEvaluator.dimensionByPath(this.dimension);
  }

  definition() {
    if (this.expression) {
      return {
        sql: this.expression,
        // TODO use actual dimension type even though it isn't used right now
        type: 'number'
      };
    }
    return this.dimensionDefinition();
  }

  aliasName() {
    // Require should be here because of cycle depend
    return this.query.escapeColumnName(this.unescapedAliasName());
  }

  unescapedAliasName() {
    if (this.expression) {
      return this.query.aliasName(this.expressionName);
    }
    return this.query.aliasName(this.dimension);
  }

  dateFieldType() {
    return this.dimensionDefinition().fieldType;
  }

  path() {
    if (this.expression) {
      return null;
    }
    if (this.query.cubeEvaluator.isSegment(this.dimension)) {
      return this.query.cubeEvaluator.parsePath('segments', this.dimension);
    }
    return this.query.cubeEvaluator.parsePath('dimensions', this.dimension);
  }
}
