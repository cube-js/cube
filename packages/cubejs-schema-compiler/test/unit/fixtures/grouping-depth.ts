// Entirely synthetic observations stored once at each hierarchy depth.
export const groupingDepthModel = (selection?: string) => `
  cube('observations', {
    sql: \`SELECT * FROM (VALUES
      (1, 'All', 'North', 'Oak',  0, 120, 12),
      (2, 'All', 'North', 'Oak',  1,  45,  3),
      (3, 'All', 'South', 'Elm',  1,  75,  9),
      (4, 'All', 'North', 'Oak',  2,  20,  1),
      (5, 'All', 'North', 'Pine', 2,  25,  2),
      (6, 'All', 'South', 'Elm',  2,  75,  9)
    ) AS source(id, root, region, site, depth, value, weight)
    WHERE \${${selection ?? `((depth) => depth === null ? '1 = 1' : 'depth = ' + depth)(
      SQL_UTILS.groupingDepth(
        ['observations.root', 'observations.region', 'observations.site'],
        'observations.depth', ['observations.value', 'observations.weight']
      ))`}}\`,
    dimensions: {
      id: { sql: 'id', type: 'number', primaryKey: true },
      root: { sql: 'root', type: 'string' },
      region: { sql: 'region', type: 'string' },
      site: { sql: 'site', type: 'string' },
      depth: { sql: 'depth', type: 'number' },
      created: { sql: "'2026-01-01'::timestamp", type: 'time' }
    },
    measures: {
      count: { type: 'count' },
      value: { sql: 'value', type: 'sum' },
      weight: { sql: 'weight', type: 'sum' },
      average: { sql: \`1.0 * \${value} / NULLIF(\${weight}, 0)\`, type: 'number' },
      by_site: { sql: \`\${value}\`, type: 'sum', multiStage: true, grain: { include: [site] } }
    }
  });
  view('report', {
    cubes: [{ joinPath: observations, includes: [
      'root', { name: 'region', alias: 'area' }, 'site', 'depth',
      'value', 'average', 'count', 'by_site'
    ] }]
  });
`;
