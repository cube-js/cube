function stringifyMemberSql(sql: any) {
  if (!sql) {
    return undefined;
  }

  const sqlStr = sql.toString();
  return sqlStr.substring(sqlStr.indexOf('=>') + 3);
}

function isVisible(member: any) {
  if (member.hasOwnProperty('shown')) {
    return member.shown;
  }

  return true;
}

function handleDimensionCaseCondition(caseCondition: any) {
  if (!caseCondition) {
    return undefined;
  }

  return {
    ...caseCondition,
    when: caseCondition?.when?.map((item) => ({
      ...item,
      sql: stringifyMemberSql(item.sql),
      label: item?.label?.sql ? stringifyMemberSql(item.label.sql) : item?.label,
    })),
  };
}

function transformCube(cube: any) {
  return {
    ...cube,
    isVisible: isVisible(cube),
    extends: stringifyMemberSql(cube?.extends),
    sql: stringifyMemberSql(cube?.sql)
  };
}

function transformMeasures(measures: any) {
  if (!measures) {
    return undefined;
  }

  return Object.entries(measures)?.map(([measureName, measure]: [measureName: string, measure: any]) => ({
    ...measure,
    name: measureName,
    isVisible: isVisible(measure),
    sql: stringifyMemberSql(measure?.sql),
    filters: measure?.filters?.map((filter) => ({
      sql: stringifyMemberSql(filter.sql),
    })),
  }));
}

function transformDimensions(dimensions: any) {
  if (!dimensions) {
    return undefined;
  }

  return Object.entries(dimensions)?.map(([dimensionName, dimension]: [dimensionName: string, dimension: any]) => ({
    ...dimension,
    name: dimensionName,
    isVisible: isVisible(dimension),
    sql: stringifyMemberSql(dimension?.sql),
    case: handleDimensionCaseCondition(dimension?.case),
  }));
}

function transformJoins(joins: any) {
  if (!joins) {
    return undefined;
  }

  return Object.entries(joins)?.map(([joinName, join]: [joinName: string, join: any]) => ({
    ...join,
    name: joinName,
    sql: stringifyMemberSql(join.sql),
  }));
}

function transformSegments(segments: any) {
  if (!segments) {
    return undefined;
  }

  return Object.entries(segments)?.map(([segmentName, segment]: [segmentName: string, segment: any]) => ({
    ...segment,
    name: segmentName,
    sql: stringifyMemberSql(segment.sql),
  }));
}

function transformPreAggregations(preAggregations: any) {
  if (!preAggregations) {
    return undefined;
  }

  return Object.entries(preAggregations)?.map(([preAggregationName, preAggregation]: [preAggregationName: string, preAggregation: any]) => ({
    ...preAggregation,
    name: preAggregationName,
    timeDimensionReference: stringifyMemberSql(preAggregation.timeDimensionReference),
    dimensionReferences: stringifyMemberSql(preAggregation.dimensionReferences),
    measureReferences: stringifyMemberSql(preAggregation.measureReferences),
  }));
}

export {
  transformCube,
  transformMeasures,
  transformDimensions,
  transformJoins,
  transformPreAggregations,
  transformSegments
};
