function stringifyMemberSql(sql?: () => string) {
  if (!sql) {
    return undefined;
  }

  const sqlStr = sql.toString();
  return sqlStr.substring(sqlStr.indexOf('=>') + 2).trim();
}

type MemberPath = {
  cubeName: string,
  memberName: string,
};

function getMemberPath(name: string): MemberPath {
  return {
    cubeName: name?.split('.')[0],
    memberName: name?.split('.')[1],
  };
}

function handleDimensionCaseCondition(caseCondition: any) {
  if (!caseCondition) {
    return undefined;
  }

  return {
    ...caseCondition,
    when: Object.values(caseCondition?.when)?.map((item: any) => ({
      ...item,
      sql: stringifyMemberSql(item.sql),
      label: item?.label?.sql ? stringifyMemberSql(item.label.sql) : item?.label,
    })),
  };
}

function transformCube(cube: any, cubeDefinitions: any) {
  return {
    ...cube,
    extends: stringifyMemberSql(cubeDefinitions[cube?.name]?.extends),
    sql: stringifyMemberSql(cubeDefinitions[cube?.name]?.sql),
    fileName: cubeDefinitions[cube?.name]?.fileName,
    refreshKey: cubeDefinitions[cube?.name]?.refreshKey,
  };
}

function transformMeasure(measure: any, cubeDefinitions: any) {
  const { cubeName, memberName } = getMemberPath(measure.name);
  return {
    ...measure,
    sql: stringifyMemberSql(cubeDefinitions[cubeName]?.measures?.[memberName]?.sql),
    filters: cubeDefinitions[cubeName]?.measures?.[memberName]?.filters?.map((filter) => ({
      sql: stringifyMemberSql(filter.sql),
    })),
  };
}

function transformDimension(dimension: any, cubeDefinitions: any) {
  const { cubeName, memberName } = getMemberPath(dimension.name);

  return {
    ...dimension,
    sql: stringifyMemberSql(cubeDefinitions[cubeName]?.dimensions?.[memberName]?.sql),
    case: handleDimensionCaseCondition(cubeDefinitions[cubeName]?.dimensions?.[memberName]?.case),
  };
}

function transformSegment(segment: any, cubeDefinitions: any) {
  const { cubeName, memberName } = getMemberPath(segment.name);
  
  return {
    ...segment,
    sql: stringifyMemberSql(cubeDefinitions[cubeName]?.segments?.[memberName]?.sql),
  };
}

function transformJoins(joins: any) {
  if (!joins) {
    return undefined;
  }

  const transformJoin = (join: any, name: string) => ({
    ...join,
    name,
    sql: stringifyMemberSql(join.sql),
  });

  // Handle joins as array (new format after PR #9800)
  if (Array.isArray(joins)) {
    return joins.map((join: any) => transformJoin(join, join.name));
  }

  // Fallback for object format (legacy)
  return Object.entries(joins)?.map(([joinName, join]: [joinName: string, join: any]) => transformJoin(join, joinName));
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
  getMemberPath,
  handleDimensionCaseCondition,
  stringifyMemberSql,
  transformCube,
  transformMeasure,
  transformDimension,
  transformSegment,
  transformJoins,
  transformPreAggregations,
};
