cube("RequestsNonReadOnly", {
  sql: `select 1 as tenant_id, 1 as deployment_id, 'req-1' as request_id, (NOW() - INTERVAL '1 day')::timestamp as timestamp
      UNION ALL
      select 2 as tenant_id, 1 as deployment_id, 'req-2' as request_id, (NOW() - INTERVAL '2 day')::timestamp as timestamp
  `,
  data_source: "postgres",
  measures: {
    count: {
      type: "count",
    },
  },
  dimensions: {
    tenant_id: {
      sql: `tenant_id`,
      type: "number",
    },
    request_id: {
      sql: `request_id`,
      type: "string",
    },
    timestamp: {
      sql: `timestamp`,
      type: "time",
    },
  },
  pre_aggregations: {
    batch_streaming_lambda: {
      type: `rollup_lambda`,
      rollups: [batch, RequestsNonReadOnlyStream.stream],
    },

    batch: {
      external: true,
      type: "rollup",
      measures: [count],
      dimensions: [tenant_id, request_id, timestamp],
      granularity: "day",
      time_dimension: RequestsNonReadOnly.timestamp,
      partition_granularity: "day",
      build_range_start: { sql: "SELECT NOW() - INTERVAL '10 day'" },
      build_range_end: { sql: "SELECT NOW()" },
    },
  },
});

cube("RequestsNonReadOnlyStream", {
  dataSource: "ksql",

  sql: `SELECT * FROM REQUESTS`,

  measures: {
    count: {
      type: "count",
    },
  },
  dimensions: {
    tenant_id: {
      sql: `TENANT_ID`,
      type: "number",
    },
    request_id: {
      sql: `REQUEST_ID`,
      type: "string",
    },
    timestamp: {
      sql: `TIMESTAMP`,
      type: "time",
    },
  },
  preAggregations: {
    stream: {
      streamOffset: "earliest",
      type: `rollup`,
      measures: [count],
      dimensions: [tenant_id, request_id, timestamp],
      time_dimension: RequestsNonReadOnlyStream.timestamp,
      granularity: "day",
      partition_granularity: "day",
      build_range_start: { sql: "SELECT DATE_SUB(NOW(), interval '96 hour')" },
      build_range_end: { sql: "SELECT NOW()" }
    },
  },
});
