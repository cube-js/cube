// Run after `cargo test -p cubesql --lib boolean_context -- --nocapture`.
// Pass its stdout as a file argument. Uses the standard MSSQL driver environment.
// All SQL reads inline VALUES; no tables or semantic models are changed.
const assert = require('node:assert/strict');
const fs = require('node:fs');
// This standalone integration check uses the package's test-only driver.
// eslint-disable-next-line import/no-extraneous-dependencies
const { MSSqlDriver } = require('@cubejs-backend/mssql-driver');

async function checkBooleanContexts(log) {
  const cases = log.split(/\r?\n/).filter(line => line.startsWith('MSSQL_BOOLEAN_CASE '))
    .map(line => JSON.parse(line.slice('MSSQL_BOOLEAN_CASE '.length)));
  assert(cases.length >= 18, 'Missing generated Rust regression cases');
  const driver = new MSSqlDriver({});
  const inputs = [true, false, null];
  const rows = inputs.flatMap((b, i) => inputs.map((c, j) => ({ id: i * 3 + j, b, c })));
  const bit = value => `CAST(${value === null ? 'NULL' : Number(value)} AS BIT)`;
  const fixture = `(VALUES ${rows.map(row => `(${row.id}, ${bit(row.b)}, ${bit(row.c)})`).join(', ')}) AS fixture(id, b, c)`;
  try {
    for (const test of cases) {
      const actual = await driver.query(`SELECT ${test.scalar} AS flag FROM ${fixture} ORDER BY id`, []);
      assert.deepEqual(actual.map(row => row.flag), test.expected, test.expression);
      const filtered = await driver.query(`SELECT id FROM ${fixture} WHERE ${test.predicate} ORDER BY id`, []);
      assert.deepEqual(filtered.map(row => Number(row.id)), rows.filter((row, i) => test.expected[i] === true).map(row => row.id), `${test.expression} filter`);
      // Group by the exact emitted scalar, including nullable predicate results.
      // Constant-only expressions are not valid SQL Server GROUP BY keys.
      if (/"[bc]"/.test(test.scalar)) {
        const grouped = await driver.query(`SELECT ${test.scalar} AS flag, COUNT(*) AS n FROM ${fixture} GROUP BY ${test.scalar}`, []);
        const expected = inputs.map(flag => ({ flag, n: test.expected.filter(value => value === flag).length })).filter(row => row.n);
        const sort = values => values.sort((a, b) => String(a.flag).localeCompare(String(b.flag)));
        assert.deepEqual(sort(grouped.map(row => ({ flag: row.flag, n: Number(row.n) }))), sort(expected), `${test.expression} grouping`);
      }
    }
    const aggregates = log.split(/\r?\n/).filter(line => line.startsWith('MSSQL_BOOLEAN_AGGREGATE '))
      .map(line => JSON.parse(line.slice('MSSQL_BOOLEAN_AGGREGATE '.length)));
    assert.equal(aggregates.length, 2, 'Missing aggregate regressions');
    for (const test of aggregates) {
      for (const empty of [false, true]) {
        const result = await driver.query(`SELECT ${test.scalar} AS flag FROM ${fixture}${empty ? ' WHERE id < 0' : ''}`, []);
        assert.deepEqual(result.map(row => row.flag), [empty ? test.expected_empty : test.expected], test.scalar);
      }
    }
    console.log(`PASS ${cases.length} generated MSSQL expressions: scalar results, filters and nonconstant grouping`);
    console.log('PASS COUNT DISTINCT and nullable SUM predicate projections, nonempty and empty inputs');
  } finally {
    await driver.release();
  }
}

module.exports = { checkBooleanContexts };
if (require.main === module) {
  checkBooleanContexts(fs.readFileSync(process.argv[2], 'utf8')).catch(error => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
