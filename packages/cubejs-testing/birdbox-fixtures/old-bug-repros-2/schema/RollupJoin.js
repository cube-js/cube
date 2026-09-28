// https://github.com/cube-js/cube/issues/11124
cube('SecondaryCube', {
  data_source: 'secondary',
  sql: `
    select '1' as id, 'loc_1' as location_id, 500 as contract_value
    UNION ALL
    select '2' as id, 'loc_2' as location_id, 300 as contract_value
  `,
  dimensions: {
    id: { sql: 'id', type: 'string', primary_key: true },
    location_id: { sql: 'location_id', type: 'string' },
  },
  measures: {
    total_contract_value: { sql: 'contract_value', type: 'sum' },
  },
  preAggregations: {
    main: {
      type: 'rollup',
      measures: [CUBE.total_contract_value],
      dimensions: [CUBE.id, CUBE.location_id],
      indexes: { by_id: { columns: [CUBE.id] } },
    },
  },
});

cube('PrimaryCube', {
  sql: `
    select '1' as id, '1' as secondary_id, 'SUCCESS' as action_status
    UNION ALL
    select '2' as id, '2' as secondary_id, 'SUCCESS' as action_status
    UNION ALL
    select '3' as id, '1' as secondary_id, 'FAILURE' as action_status
  `,
  joins: {
    SecondaryCube: {
      // Member references, as rollup_join requires
      sql: `${CUBE.secondary_id} = ${SecondaryCube.id}`,
      relationship: 'many_to_one',
    },
  },
  dimensions: {
    id: { sql: 'id', type: 'string', primary_key: true },
    secondary_id: { sql: 'secondary_id', type: 'string' },
    action_status: { sql: 'action_status', type: 'string' },
  },
  measures: {
    total_count: {
      type: 'count',
      filters: [{ sql: `${CUBE}.action_status = 'SUCCESS'` }],
    },
  },
  preAggregations: {
    primary_rollup: {
      type: 'rollup',
      measures: [CUBE.total_count],
      dimensions: [CUBE.secondary_id, CUBE.action_status],
      indexes: { by_secondary_id: { columns: [CUBE.secondary_id] } },
    },
    joined_rollup: {
      type: 'rollup_join',
      rollups: [PrimaryCube.primary_rollup, SecondaryCube.main],
      measures: [PrimaryCube.total_count, SecondaryCube.total_contract_value],
      dimensions: [PrimaryCube.secondary_id, SecondaryCube.location_id],
    },
  },
});
