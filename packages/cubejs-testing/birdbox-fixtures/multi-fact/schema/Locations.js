// Shared dimension cube. Both facts join to it, which is what gives a
// multi-fact query something to stitch the two aggregates on.
cube(`Locations`, {
  sql: `
  select 1 as id, 'West' as region
  UNION ALL
  select 2 as id, 'East' as region
  `,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    region: {
      sql: `region`,
      type: `string`,
    },
  },
});
