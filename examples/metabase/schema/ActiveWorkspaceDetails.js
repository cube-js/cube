cube(`ActiveWorkspaceDetails`, {
  sql: `SELECT * FROM public.active_workspace_details`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: []
    }
  },
  
  dimensions: {
    isActive: {
      sql: `is_active`,
      type: `string`
    },
    
    reportingDay: {
      sql: `reporting_day`,
      type: `time`
    }
  },
  
  dataSource: `default`
});
