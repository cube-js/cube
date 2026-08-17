import { PostgresQuery } from '../../src';
import { prepareJsCompiler } from './PrepareCompiler';

// Regression test: an access_policy row_level filter on a cube that is only
// reached via a LEFT JOIN gets pushed into the query's top-level `filters`
// (see CompilerApi.ts `rlsFilter` push). Because query.filters compiles to a
// WHERE-clause predicate applied *after* the LEFT JOIN, a filter on the
// right-hand (joined) cube's column silently turns the LEFT JOIN into an
// INNER JOIN: rows from the left cube (orders) that have no matching row
// satisfying the filter on the right cube (shipments) are dropped entirely,
// instead of being kept with NULL shipment columns and then correctly
// excluded/kept per RLS semantics on the shipment fields only.
describe('RLS row_level filter on a LEFT-JOINed cube', () => {
  const schema = `
    cube(\`orders\`, {
      sql_table: \`orders_tbl\`,
      joins: {
        shipments: {
          sql: \`\${CUBE}.id = \${shipments}.order_id\`,
          relationship: \`one_to_one\`,
        },
      },
      measures: {
        count: { type: \`count\` },
      },
      dimensions: {
        id: { sql: \`id\`, type: \`number\`, primaryKey: true },
        status: { sql: \`status\`, type: \`string\` },
      },
    });

    cube(\`shipments\`, {
      sql_table: \`shipments_tbl\`,
      measures: {},
      dimensions: {
        id: { sql: \`id\`, type: \`number\`, primaryKey: true },
        order_id: { sql: \`order_id\`, type: \`number\` },
        carrier: { sql: \`carrier\`, type: \`string\` },
      },
    });
  `;

  it('reproduces: RLS filter on joined cube is emitted as a post-JOIN WHERE predicate', async () => {
    const compilers = prepareJsCompiler(schema);
    await compilers.compiler.compile();

    // Simulate what CompilerApi's checkAccessOrThrow/applyRowLevelSecurity
    // does: it computes an RLS predicate for `shipments` (e.g. from
    // access_policy.row_level.filters restricting to a carrier the current
    // user is allowed to see) and pushes it into query.filters alongside the
    // user's own query — this is the `query.filters.push(rlsFilter)` call
    // seen in CompilerApi.ts around the rlsFilter handling.
    const rlsFilter = { member: 'shipments.carrier', operator: 'equals', values: ['ups'] };

    const query = new PostgresQuery(compilers, {
      measures: ['orders.count'],
      dimensions: ['orders.status'],
      filters: [rlsFilter],
      timezone: 'UTC',
    });

    const sql = query.buildSqlAndParams()[0];
    // eslint-disable-next-line no-console
    console.log(sql);

    expect(sql).toContain('LEFT JOIN');

    const whereIdx = sql.search(/WHERE/i);
    const joinIdx = sql.search(/LEFT JOIN/i);
    expect(joinIdx).toBeGreaterThan(-1);
    expect(whereIdx).toBeGreaterThan(joinIdx);

    // The bug: the shipments.carrier predicate lands in the WHERE clause,
    // not in the JOIN's ON condition. Since SQL evaluates WHERE after the
    // LEFT JOIN and NULL = 'ups' is neither true, any order row with no
    // shipment (or a shipment from a different carrier) is dropped from the
    // result set entirely — exactly like an INNER JOIN would behave, even
    // though the join is declared LEFT.
    const whereClause = sql.slice(whereIdx);
    expect(whereClause).toMatch(/carrier/i);

    // Demonstrates the fix would require the predicate to be part of the
    // JOIN condition instead. Confirm today's (buggy) SQL does NOT do that:
    const onClauseText = sql.slice(joinIdx, whereIdx);
    expect(onClauseText).not.toMatch(/carrier/i);
  });
});
