class BaseDimension {
  constructor(query, dimension) {
    this.query = query;
    this.dimension = dimension;
  }

  selectColumns() {
    return [`${this.dimensionSql()} ${this.aliasName()}`];
  }

  cumulativeSelectColumns() {
    return [`${this.aliasName()}`];
  }

  dimensionSql() {
    return this.query.dimensionSql(this);
  }

  sqlDefinition() {
    return this.dimensionDefinition().sql;
  }

  cube() {
    return this.query.cubeEvaluator.cubeFromPath(this.dimension);
  }

  dimensionDefinition() {
    if (this.query.cubeEvaluator.isSegment(this.dimension)) {
      return this.query.cubeEvaluator.segmentByPath(this.dimension);
    }
    return this.query.cubeEvaluator.dimensionByPath(this.dimension);
  }

  definition() {
    return this.dimensionDefinition();
  }

  aliasName() {
    // Require should be here because of cycle depend
    return this.query.escapeColumnName(this.unescapedAliasName());
  }

  unescapedAliasName() {
    return this.query.aliasName(this.dimension);
  }

  dateFieldType() {
    return this.dimensionDefinition().fieldType;
  }

  path() {
    if (this.query.cubeEvaluator.isSegment(this.dimension)) {
      return this.query.cubeEvaluator.parsePath('segments', this.dimension);
    }
    return this.query.cubeEvaluator.parsePath('dimensions', this.dimension);
  }
}

module.exports = BaseDimension;