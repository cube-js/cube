class BaseSegment {
  constructor(query, segment) {
    this.query = query;
    this.segment = segment;
  }

  filterToWhere() {
    return this.query.segmentSql(this);
  }

  filterParams() {
    return [];
  }

  segmentDefinition() {
    return this.query.cubeEvaluator.segmentByPath(this.segment);
  }

  definition() {
    return this.segmentDefinition();
  }

  cube() {
    return this.query.cubeEvaluator.cubeFromPath(this.segment);
  }

  sqlDefinition() {
    return this.segmentDefinition().sql;
  }

  path() {
    return this.query.cubeEvaluator.parsePath('segments', this.segment);
  }
}

module.exports = BaseSegment;