const CUBE_ALIAS_MAPPING = {
  sql_alias: 'sqlAlias',
  sql_table: 'sqlTable',
  refresh_key: 'refreshKey',
  pre_aggregations: 'preAggregations',
  rewrite_queries: 'rewriteQueries',
  data_source: 'dataSource',
};

const CUBE_MEASURES_ALIAS_MAPPING = {
  drill_members: 'drillMembers',
  rolling_window: 'rollingWindow',
};

const CUBE_DIMENSIONS_ALIAS_MAPPING = {
  primary_key: 'primaryKey',
  propagate_filters_to_sub_query: 'propagateFiltersToSubQuery',
  sub_query: 'subQuery',
};

const CUBE_PRE_AGGREGATION_ALIAS_MAPPING = {
  time_dimension: 'timeDimension',
  partition_granularity: 'partitionGranularity',
  refresh_key: 'refreshKey',
  allow_non_strict_date_range_match: 'allowNonStrictDateRangeMatch',
  use_original_sql_pre_aggregations: 'useOriginalSqlPreAggregations',
  scheduled_refresh: 'scheduledRefresh',
  build_range_start: 'buildRangeStart',
  build_range_end: 'buildRangeEnd',
  union_with_source_data: 'unionWithSourceData',
};

function transformAliases(obj, mapping, nested = true) {
  if (!obj) {
    return;
  }

  if (nested) {
    for (const field of Object.keys(obj)) {
      transformAliases(obj[field], mapping, false);
    }
  } else {
    for (const field of Object.keys(mapping)) {
      if (obj[field] !== undefined) {
        obj[mapping[field]] = obj[field];
        delete obj[field];
      }
    }
  }
}

export function camelizeCube(cube: any) {
  transformAliases(cube, CUBE_ALIAS_MAPPING, false);
  transformAliases(cube.measures, CUBE_MEASURES_ALIAS_MAPPING);
  transformAliases(cube.dimensions, CUBE_DIMENSIONS_ALIAS_MAPPING);
  transformAliases(cube.preAggregations, CUBE_PRE_AGGREGATION_ALIAS_MAPPING);
}
