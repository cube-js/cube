import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// #10864: a multi_stage `type: number` measure whose SQL contains inline
// aggregate functions over another measure (e.g. STDDEV({x}) / AVG({x})),
// queried by a dimension that is not in add_group_by, must be re-aggregated
// with a GROUP BY on the query dimension in the outer multi-stage CTE.
// Tesseract emits the aggregate expressions together with the dimension column
// but no GROUP BY, which databases reject.
describe('Issue #10864 - multi_stage type number with inline aggregates loses GROUP BY', () => {
  const model = `
cubes:
  - name: Orders
    sql: >
      SELECT 1 AS id, 100 AS amount, 'new' AS status UNION ALL
      SELECT 2 AS id, 200 AS amount, 'new' AS status UNION ALL
      SELECT 3 AS id, 300 AS amount, 'processed' AS status
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
    measures:
      - name: amount_count
        sql: amount
        type: count
      - name: cv_amount
        sql: "STDDEV({amount_count}) / NULLIF(AVG({amount_count}), 0)"
        type: number
        multi_stage: true
        add_group_by:
          - id
`;

  it('groups the outer multi-stage aggregation by the query dimension', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.cv_amount'],
      dimensions: ['Orders.status'],
      timezone: 'UTC',
    });
    const [sql] = query.buildSqlAndParams();

    // Locate the SELECT that computes the STDDEV/AVG expression and ensure it has
    // a GROUP BY (it selects the status column alongside aggregates).
    const normalized = sql.replace(/\s+/g, ' ');
    const stddevIdx = normalized.search(/stddev\(/i);
    expect(stddevIdx).toBeGreaterThan(-1);
    const selectStart = normalized.toLowerCase().lastIndexOf('select', stddevIdx);
    // The end of this SELECT is the closing paren of its CTE, or end of query.
    let depth = 0;
    let end = normalized.length;

    for (let i = selectStart; i < normalized.length; i++) {
      if (normalized[i] === '(') depth++;
      if (normalized[i] === ')') {
        if (depth === 0) {
          end = i;
          break;
        }
        depth--;
      }
    }
    const aggregatingSelect = normalized.substring(selectStart, end);
    expect(aggregatingSelect).toMatch(/GROUP BY/i);
  });
});
