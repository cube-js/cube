import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// A member's `sql` is an arbitrary expression, and a filter template splices it
// next to an operator of its own. When the member's own top-level operator binds
// weaker than the filter's, the unparenthesized splice re-associates: the filter
// operator captures only the tail of the member expression. That is a syntax
// error on some dialects and — for `AND`/`OR` members — valid SQL over a
// different row set on all of them.
//
// Inline data (6 rows):
//   id  grp  amount  flag   note
//   1   g1   100     true   'alpha'
//   2   g1   10      true   NULL
//   3   g2   200     false  'beta'
//   4   g2   10      false  NULL
//   5   g3   100     NULL   'gamma'
//   6   g3   10      NULL   NULL
//
// `big_and_flag` = `amount > 50 AND flag` evaluates to
//   1: true  2: false  3: false  4: false  5: NULL  6: false
// and `big_or_flag` = `amount > 50 OR flag` to
//   1: true  2: true  3: true  4: false  5: true  6: NULL
describe('Filter member SQL parenthesization', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT * FROM (VALUES
        (1, 'g1', 100, TRUE,  'alpha', '2024-01-01'::timestamp),
        (2, 'g1', 10,  TRUE,  NULL,    '2024-01-02'::timestamp),
        (3, 'g2', 200, FALSE, 'beta',  '2024-01-03'::timestamp),
        (4, 'g2', 10,  FALSE, NULL,    '2024-01-04'::timestamp),
        (5, 'g3', 100, NULL,  'gamma', '2024-01-05'::timestamp),
        (6, 'g3', 10,  NULL,  NULL,    '2024-01-06'::timestamp)
      ) AS t(id, grp, amount, flag, note, created_at)

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
        public: true

      - name: grp
        sql: grp
        type: string

      - name: amount
        sql: amount
        type: number

      # Top-level AND — binds weaker than every filter operator.
      - name: big_and_flag
        sql: "amount > 50 AND flag"
        type: boolean

      # Top-level OR — same, and keeps NULL propagation visible.
      - name: big_or_flag
        sql: "amount > 50 OR flag"
        type: boolean

      # Top-level comparison — Postgres rejects a second comparison next to it.
      - name: big
        sql: "amount > 50"
        type: boolean

      - name: amount_plus
        sql: "amount + 1"
        type: number

      - name: note_tagged
        sql: "note || '-tag'"
        type: string

      - name: amount_plus_commented
        sql: "amount + 1 -- one more"
        type: number

      - name: shifted_at
        sql: "created_at + interval '1 day'"
        type: time

    measures:
      - name: count
        type: count

      - name: total
        sql: amount
        type: sum

      - name: flag_any
        sql: "bool_or(flag)"
        type: boolean

      # Top-level AND over two aggregates.
      - name: busy
        sql: "{total} > 150 AND {flag_any}"
        type: boolean

      # Top-level comparison over an aggregate.
      - name: total_over_150
        sql: "{total} > 150"
        type: boolean

      # The reported model's shape: a calculated boolean over a plain measure.
      - name: total_is_set
        sql: "{total} IS NOT NULL"
        type: boolean
  `);

  async function buildSql(q: any): Promise<[string, any[]]> {
    await compilers.compiler.compile();
    const query = new PostgresQuery(compilers, {
      ...q,
      timezone: 'UTC',
      preAggregationsSchema: '',
    });
    return query.buildSqlAndParams() as [string, any[]];
  }

  // Executes the query and compares the row set. Returns the generated SQL so a
  // caller can additionally assert its shape.
  async function expectRows(q: any, expected: any[]): Promise<string> {
    const sqlAndParams = await buildSql(q);
    const res = await dbRunner.testQuery(sqlAndParams);
    expect(res).toEqual(expected);
    return sqlAndParams[0];
  }

  const idRows = (...ids: number[]) => ids.map(id => ({ orders__id: id, orders__count: '1' }));

  const byId = (filters: any[]) => ({
    measures: ['orders.count'],
    dimensions: ['orders.id'],
    filters,
    order: [{ id: 'orders.id' }],
  });

  describe('WHERE — dimension whose SQL is a top-level AND/OR', () => {
    it('equals', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byId([{ member: 'orders.big_and_flag', operator: 'equals', values: ['false'] }]),
        idRows(2, 3, 4, 6)
      );
    });

    it('notEquals', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byId([{ member: 'orders.big_and_flag', operator: 'notEquals', values: ['true'] }]),
        idRows(2, 3, 4, 5, 6)
      );
    });

    it('equals with several values (IN list)', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byId([{ member: 'orders.big_and_flag', operator: 'equals', values: ['true', 'false'] }]),
        idRows(1, 2, 3, 4, 6)
      );
    });

    it('notEquals with several values (NOT IN list)', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byId([{ member: 'orders.big_or_flag', operator: 'notEquals', values: ['true', 'false'] }]),
        idRows(6)
      );
    });

    it('set', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byId([{ member: 'orders.big_and_flag', operator: 'set' }]),
        idRows(1, 2, 3, 4, 6)
      );
    });

    it('notSet', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byId([{ member: 'orders.big_or_flag', operator: 'notSet' }]),
        idRows(6)
      );
    });
  });

  describe('WHERE — dimension whose SQL is a top-level comparison', () => {
    // Unparenthesized this renders `amount > 50 = CAST($1 AS BOOLEAN)`, which
    // Postgres rejects outright ("syntax error at or near =").
    it('equals', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byId([{ member: 'orders.big', operator: 'equals', values: ['false'] }]),
        idRows(2, 4, 6)
      );
    });

    it('notEquals', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byId([{ member: 'orders.big', operator: 'notEquals', values: ['true'] }]),
        idRows(2, 4, 6)
      );
    });
  });

  describe('HAVING — measure whose SQL is a top-level AND/comparison', () => {
    const byGrp = (filters: any[]) => ({
      measures: ['orders.total'],
      dimensions: ['orders.grp'],
      filters,
      order: [{ id: 'orders.grp' }],
    });

    it('equals on a top-level AND measure', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byGrp([{ member: 'orders.busy', operator: 'equals', values: ['false'] }]),
        [
          { orders__grp: 'g1', orders__total: '110' },
          { orders__grp: 'g2', orders__total: '210' },
          { orders__grp: 'g3', orders__total: '110' },
        ]
      );
    });

    it('equals on a top-level comparison measure', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      await expectRows(
        byGrp([{ member: 'orders.total_over_150', operator: 'equals', values: ['true'] }]),
        [{ orders__grp: 'g2', orders__total: '210' }]
      );
    });

    // The reported model: `sum(...) IS NOT NULL = CAST($1 AS BOOLEAN)` is a
    // syntax error on Trino/Athena. Postgres happens to parse it the intended
    // way, so only the emitted shape can be asserted here.
    it('equals on a top-level IS NOT NULL measure', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      const sql = await expectRows(
        byGrp([{ member: 'orders.total_is_set', operator: 'equals', values: ['true'] }]),
        [
          { orders__grp: 'g1', orders__total: '110' },
          { orders__grp: 'g2', orders__total: '210' },
          { orders__grp: 'g3', orders__total: '110' },
        ]
      );
      expect(sql).toContain('(sum("orders".amount) IS NOT NULL) =');
    });
  });

  // The remaining operators cannot be made to diverge on Postgres — its
  // precedence happens to agree with the intended reading — but they splice the
  // member the same way, so the wrapping is asserted on the emitted SQL.
  describe('emitted SQL wraps a compound member for every operator', () => {
    // Each expectation is the whole rendered predicate, so a case also pins the
    // operator and pattern it belongs to rather than only the parentheses.
    const NUM = '(amount + 1)';
    const STR = '(note || \'-tag\')';
    const TS = '(created_at + interval \'1 day\')';

    const cases: Array<[string, any, string]> = [
      ['gt', { member: 'orders.amount_plus', operator: 'gt', values: ['10'] }, `WHERE (${NUM} > $1)`],
      ['gte', { member: 'orders.amount_plus', operator: 'gte', values: ['10'] }, `WHERE (${NUM} >= $1)`],
      ['lt', { member: 'orders.amount_plus', operator: 'lt', values: ['10'] }, `WHERE (${NUM} < $1)`],
      ['lte', { member: 'orders.amount_plus', operator: 'lte', values: ['10'] }, `WHERE (${NUM} <= $1)`],
      ['contains', { member: 'orders.note_tagged', operator: 'contains', values: ['alpha'] },
        `WHERE ((${STR} ILIKE '%' || $1|| '%'))`],
      ['notContains', { member: 'orders.note_tagged', operator: 'notContains', values: ['alpha'] },
        `WHERE ((${STR} NOT ILIKE '%' || $1|| '%') OR ${STR} IS NULL)`],
      ['startsWith', { member: 'orders.note_tagged', operator: 'startsWith', values: ['alpha'] },
        `WHERE ((${STR} ILIKE $1|| '%'))`],
      ['endsWith', { member: 'orders.note_tagged', operator: 'endsWith', values: ['tag'] },
        `WHERE ((${STR} ILIKE '%' || $1))`],
      ['inDateRange', {
        member: 'orders.shifted_at',
        operator: 'inDateRange',
        values: ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999'],
      }, `WHERE (${TS} >= $1::timestamptz AND ${TS} <= $2::timestamptz)`],
      ['notInDateRange', {
        member: 'orders.shifted_at',
        operator: 'notInDateRange',
        values: ['2024-01-01T00:00:00.000', '2024-01-31T23:59:59.999'],
      }, `WHERE (${TS} < $1::timestamptz OR ${TS} > $2::timestamptz)`],
      ['beforeDate', {
        member: 'orders.shifted_at',
        operator: 'beforeDate',
        values: ['2024-01-31T23:59:59.999'],
      }, `WHERE (${TS} < $1::timestamptz)`],
      ['afterDate', {
        member: 'orders.shifted_at',
        operator: 'afterDate',
        values: ['2024-01-01T00:00:00.000'],
      }, `WHERE (${TS} > $1::timestamptz)`],
    ];

    it.each(cases)('%s', async (_name, filter, expected) => {
      if (!getEnv('nativeSqlPlanner')) return;
      const [sql] = await buildSql(byId([filter]));
      expect(sql).toContain(expected);
    });
  });

  // A member SQL ending in a line comment would comment out the closing
  // parenthesis, so it gets a line of its own.
  it('wraps a member whose SQL ends in a line comment', async () => {
    if (!getEnv('nativeSqlPlanner')) return;
    const sql = await expectRows(
      byId([{ member: 'orders.amount_plus_commented', operator: 'gt', values: ['50'] }]),
      idRows(1, 3, 5)
    );
    expect(sql).toContain('(amount + 1 -- one more\n) > $1');
  });

  // The wrapping must stay off atomic members, or every filter in every model
  // would change shape.
  describe('atomic members stay unwrapped', () => {
    it('plain column dimension', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      const [sql] = await buildSql(byId([
        { member: 'orders.amount', operator: 'equals', values: ['100'] },
      ]));
      expect(sql).toContain('"orders".amount = ');
      expect(sql).not.toContain('("orders".amount) = ');
    });

    it('aggregate measure', async () => {
      if (!getEnv('nativeSqlPlanner')) return;
      const [sql] = await buildSql({
        measures: ['orders.total'],
        dimensions: ['orders.grp'],
        filters: [{ member: 'orders.total', operator: 'gt', values: ['100'] }],
      });
      expect(sql).toContain('sum("orders".amount) > ');
      expect(sql).not.toContain('(sum("orders".amount)) > ');
    });
  });
});
