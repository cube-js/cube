import booleanFixture from '../../fixtures/mssql-boolean-contexts.json';
import { dbRunner } from './MSSqlDbRunner';

// Rust asserts these SQL strings against its actual expression renderer, and the
// adapter unit test checks the fixture's templates against MssqlQuery. Expected
// values are also checked against independent three-valued truth tables in Rust.
describe('MSSQL SQL API boolean contexts', () => {
  jest.setTimeout(200000);
  const inputs = [true, false, null];
  const rows = inputs.flatMap((b, i) => inputs.map((c, j) => ({ id: i * 3 + j, b, c })));
  const bit = (value: boolean | null) => `CAST(${value === null ? 'NULL' : Number(value)} AS BIT)`;
  const fixture = `(VALUES ${rows.map(row => `(${row.id}, ${bit(row.b)}, ${bit(row.c)})`).join(', ')}) AS fixture(id, b, c)`;
  const query = (sql: string) => dbRunner.testQuery([sql, []]);

  it.each(booleanFixture.cases)('preserves scalar, filter and grouping results for $expression', async test => {
    const actual = await query(`SELECT ${test.scalar} AS flag FROM ${fixture} ORDER BY id`);
    expect(actual.map(row => row.flag)).toEqual(test.expected);

    const filtered = await query(`SELECT id FROM ${fixture} WHERE ${test.predicate} ORDER BY id`);
    expect(filtered.map(row => Number(row.id))).toEqual(rows.filter((row, i) => test.expected[i] === true).map(row => row.id));

    // Constant-only GROUP BY is unsupported in SQL Server.
    if (/"[bc]"/.test(test.scalar)) {
      const grouped = await query(`SELECT ${test.scalar} AS flag, COUNT(*) AS n FROM ${fixture} GROUP BY ${test.scalar}`);
      const expected = inputs.map(flag => ({ flag, n: test.expected.filter(value => value === flag).length })).filter(row => row.n);
      expect(grouped.map(row => ({ flag: row.flag, n: Number(row.n) }))).toEqual(expect.arrayContaining(expected));
      expect(grouped).toHaveLength(expected.length);
    }
  });

  it.each(booleanFixture.aggregates)('preserves aggregate projection $scalar on nonempty and empty inputs', async test => {
    for (const empty of [false, true]) {
      const result = await query(`SELECT ${test.scalar} AS flag FROM ${fixture}${empty ? ' WHERE id < 0' : ''}`);
      expect(result.map(row => row.flag)).toEqual([empty ? test.expected_empty : test.expected]);
    }
  });
});
