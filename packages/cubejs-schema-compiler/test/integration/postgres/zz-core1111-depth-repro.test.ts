// CORE-1111 reproduction (scratch, not for commit): a view whose rows exist
// once per ancestor depth `d` must read the depth of the deepest grouped
// level. `default_filters.unless` is released only by filters, so grouping
// cannot select the depth natively.
import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('CORE-1111 grouping-scope depth', () => {
  jest.setTimeout(200000);

  // R -> A -> {A1 = 10, A2 = 3}, R -> B = 4. A2 adds -1 into A and R but +1
  // at its own depth; B adds -1 into R and +1 at its own depth.
  // Correct display: R = 3, A = 7, B = 4, A1 = 10, A2 = 3.
  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: acct
    sql: >
      SELECT * FROM (VALUES
        ('A1', 10, 0,  1, 'R', 'A', 'A1'),
        ('A1', 10, 1,  1, 'R', 'A', 'A1'),
        ('A1', 10, 2,  1, 'R', 'A', 'A1'),
        ('A2',  3, 0, -1, 'R', 'A', 'A2'),
        ('A2',  3, 1, -1, 'R', 'A', 'A2'),
        ('A2',  3, 2,  1, 'R', 'A', 'A2'),
        ('B',   4, 0, -1, 'R', 'B', NULL),
        ('B',   4, 1,  1, 'R', 'B', NULL),
        ('B',   4, 2,  0, 'R', 'B', NULL)
      ) AS t(leaf, amount, d, sign, level0, level1, level2)
    dimensions:
      - name: rid
        sql: "leaf || '|' || d"
        type: string
        primary_key: true
      - name: d
        sql: d
        type: number
      - name: level0
        sql: level0
        type: string
      - name: level1
        sql: level1
        type: string
      - name: level2
        sql: level2
        type: string
    measures:
      - name: amount
        sql: amount * sign
        type: sum

views:
  - name: acct_v
    cubes:
      - join_path: acct
        includes: [d, level0, level1, level2, amount]
    default_filters:
      - member: d
        operator: lt
        values: ["1"]
        unless: [level1]
      - member: d
        operator: lt
        values: ["2"]
        unless: [level2]
  `);

  async function run(q: any) {
    await compiler.compile();
    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      { ...q, timezone: 'UTC', preAggregationsSchema: '' }
    );
    const [sql, params] = query.buildSqlAndParams();
    const rows = await dbRunner.testQuery([sql, params]);
    // eslint-disable-next-line no-console
    console.log(`${JSON.stringify(q)}\nSQL: ${sql}\nPARAMS: ${JSON.stringify(params)}\nROWS: ${JSON.stringify(rows)}`);
    return rows.map((r: any) => Object.fromEntries(
      Object.entries(r).map(([k, v]) => [k.replace('acct_v__', ''), k.endsWith('amount') ? Number(v) : v])
    ));
  }

  const lv = (n: number) => Array.from({ length: n + 1 }, (_, i) => `acct_v.level${i}`);
  const order = (n: number) => lv(n).map(id => ({ id }));
  const depth = (k: number) => ({ member: 'acct_v.d', operator: 'equals', values: [String(k)] });

  it('planner is Tesseract', () => expect(getEnv('nativeSqlPlanner')).toBe(true));

  describe('no hook (native today)', () => {
    it('root only: guards pin d=0 -> R = 3 (correct)', async () => {
      expect(await run({ measures: ['acct_v.amount'], dimensions: lv(0) }))
        .toEqual([{ level0: 'R', amount: 3 }]);
    });

    it('level1 grouped: guards still pin d=0 -> B shows R\'s sign', async () => {
      expect(await run({ measures: ['acct_v.amount'], dimensions: lv(1), order: order(1) }))
        .toEqual([{ level0: 'R', level1: 'A', amount: 7 }, { level0: 'R', level1: 'B', amount: 4 }]);
    });

    it('level2 grouped: guards still pin d=0 -> A2 shows its parent sign', async () => {
      expect(await run({ measures: ['acct_v.amount'], dimensions: lv(2), order: order(2) }))
        .toEqual([
          { level0: 'R', level1: 'A', level2: 'A1', amount: 10 },
          { level0: 'R', level1: 'A', level2: 'A2', amount: 3 },
          { level0: 'R', level1: 'B', level2: null, amount: 0 },
        ]);
    });

    it('root grouped + deep business filter level1=A: filter releases a guard -> depths summed', async () => {
      expect(await run({
        measures: ['acct_v.amount'],
        dimensions: lv(0),
        filters: [{ member: 'acct_v.level1', operator: 'equals', values: ['A'] }],
      })).toEqual([{ level0: 'R', amount: 7 }]);
    });

    it('root grouped + leaf filter level1=A, level2=A2: both guards released -> all depths summed (-3-3+3, equals R's -3 only by coincidence; see SQL)', async () => {
      expect(await run({
        measures: ['acct_v.amount'],
        dimensions: lv(0),
        filters: [
          { member: 'acct_v.level1', operator: 'equals', values: ['A'] },
          { member: 'acct_v.level2', operator: 'equals', values: ['A2'] },
        ],
      })).toEqual([{ level0: 'R', amount: -3 }]);
    });
  });

  describe('control: explicit d = deepest grouped level (what E-DEPTH injects)', () => {
    it('level1 grouped, d=1', async () => {
      expect(await run({ measures: ['acct_v.amount'], dimensions: lv(1), order: order(1), filters: [depth(1)] }))
        .toEqual([{ level0: 'R', level1: 'A', amount: 7 }, { level0: 'R', level1: 'B', amount: 4 }]);
    });

    it('level2 grouped, d=2', async () => {
      expect(await run({ measures: ['acct_v.amount'], dimensions: lv(2), order: order(2), filters: [depth(2)] }))
        .toEqual([
          { level0: 'R', level1: 'A', level2: 'A1', amount: 10 },
          { level0: 'R', level1: 'A', level2: 'A2', amount: 3 },
          { level0: 'R', level1: 'B', level2: null, amount: 0 },
        ]);
    });

    it('root grouped + level1=A filter, d=0', async () => {
      expect(await run({
        measures: ['acct_v.amount'],
        dimensions: lv(0),
        filters: [{ member: 'acct_v.level1', operator: 'equals', values: ['A'] }, depth(0)],
      })).toEqual([{ level0: 'R', amount: 7 }]);
    });

    it('negative control: root grouped + A2 filter, deepest-FILTER depth d=2 -> wrong +3', async () => {
      expect(await run({
        measures: ['acct_v.amount'],
        dimensions: lv(0),
        filters: [
          { member: 'acct_v.level1', operator: 'equals', values: ['A'] },
          { member: 'acct_v.level2', operator: 'equals', values: ['A2'] },
          depth(2),
        ],
      })).toEqual([{ level0: 'R', amount: 3 }]);
    });
  });
});
