cube("Requests", {
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
      primaryKey: true,
    },
    request_id: {
      sql: `request_id`,
      type: "string",
      primaryKey: true,
    },
    timestamp: {
      sql: `timestamp`,
      type: "time",
    },
  },
  pre_aggregations: {
    batch_streaming_lambda: {
      type: `rollup_lambda`,
      rollups: [batch, RequestsStream.stream],
    },

    batch: {
      external: true,
      type: "rollup",
      measures: [count],
      dimensions: [tenant_id, request_id, timestamp],
      granularity: "day",
      time_dimension: Requests.timestamp,
      partition_granularity: "day",
      build_range_start: { sql: "SELECT NOW() - INTERVAL '10 day'" },
      build_range_end: { sql: "SELECT NOW()" },
    },
  },
});

cube("RequestsStream", {
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
      primaryKey: true,
    },
    request_id: {
      sql: `REQUEST_ID`,
      type: "string",
      primaryKey: true,
    },
    timestamp: {
      sql: `TIMESTAMP`,
      type: "time",
    },
  },
  preAggregations: {
    stream: {
      streamOffset: "earliest",
      readOnly: true,
      external: true,
      type: `rollup`,
      measures: [count],
      dimensions: [tenant_id, request_id, timestamp],
      time_dimension: RequestsStream.timestamp,
      granularity: "day",
      unique_key_columns: ["tenant_id", "request_id"],
      partition_granularity: "day",
      build_range_start: { sql: "SELECT DATE_SUB(NOW(), interval '96 hour')" },
      build_range_end: { sql: "SELECT NOW()" },
      outputColumnTypes: [
        { name: "tenant_id", type: "int" },
        { name: "request_id", type: "text" },
        { name: "timestamp", type: "timestamp" },
        { name: "count", type: "int" },
      ],
    },
  },
});
