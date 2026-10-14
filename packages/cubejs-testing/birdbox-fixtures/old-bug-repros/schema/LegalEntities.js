// cube-js/cube#9462: `string` measure (STRING_AGG) in a rollup must not be re-aggregated with SUM.
// `LENoPa` is an identical cube without pre-aggregations (baseline).
const LE_SQL = `
  SELECT 'e1' AS legal_entity_id, 'Alice' AS display_name, 'person' AS entity_type UNION ALL
  SELECT 'e1', 'Bob', 'person' UNION ALL
  SELECT 'e2', 'Carol', 'company' UNION ALL
  SELECT 'e3', 'Dave', 'company' UNION ALL
  SELECT 'e3', 'Eve', 'company'
`;
cube(`LE`, {
  sql: LE_SQL,
  measures: {
    display_names: { sql: `STRING_AGG(${CUBE.display_name}::TEXT, ', ' ORDER BY display_name)`, type: `string` },
  },
  dimensions: {
    legal_entity_id: { sql: `legal_entity_id`, type: `string`, primary_key: true, shown: true },
    entity_type: { sql: `entity_type`, type: `string` },
    display_name: { sql: `display_name`, type: `string` },
  },
  preAggregations: {
    rollup: {
      dimensions: [CUBE.legal_entity_id],
      measures: [CUBE.display_names],
      indexes: { idx: { columns: [CUBE.legal_entity_id] } },
      refresh_key: { every: `1 hour` },
    }
  },
});

cube(`LENoPa`, {
  sql: LE_SQL,
  measures: {
    display_names: { sql: `STRING_AGG(${CUBE.display_name}::TEXT, ', ' ORDER BY display_name)`, type: `string` },
  },
  dimensions: {
    legal_entity_id: { sql: `legal_entity_id`, type: `string`, primary_key: true, shown: true },
    display_name: { sql: `display_name`, type: `string` },
  },
});
