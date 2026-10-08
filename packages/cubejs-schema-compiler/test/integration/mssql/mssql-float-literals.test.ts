import { fork } from 'child_process';
import path from 'path';
import type { CompilationRequest, CompilationResponse, SourceQuery } from './mssql-float-literals-compiler';
import { dbRunner } from './MSSqlDbRunner';

const compileQueries = (request: CompilationRequest): Promise<Exclude<CompilationResponse, { error: string }>> => new Promise((resolve, reject) => {
  const worker = fork(path.join(__dirname, 'mssql-float-literals-compiler.js'), [], { stdio: ['ignore', 'inherit', 'inherit', 'ipc'] });
  let response: CompilationResponse | undefined;
  const timeout = setTimeout(() => {
    worker.kill();
    reject(new Error('SQL compilation worker timed out'));
  }, 150000);
  worker.once('message', (message: CompilationResponse) => { response = message; });
  worker.once('error', error => {
    clearTimeout(timeout);
    reject(error);
  });
  worker.once('close', code => {
    clearTimeout(timeout);
    if (response && 'error' in response) reject(new Error(response.error));
    else if (code !== 0 || !response) reject(new Error(`SQL compilation worker exited with code ${code}`));
    else resolve(response);
  });
  worker.send(request);
});

describe.each([false, true])('MSSQL float literals (native planner: %s)', useNativeSqlPlanner => {
  jest.setTimeout(200000);
  const from = 'FROM Visitors WHERE id > 0';
  const cases = [
    { name: 'fractional percentage', expression: '100.0 * COUNT(*) / (3 * COUNT(*))', expected: 100 / 3, literal: /CAST\(100 AS DECIMAL\(10, 0\)\)/ },
    { name: 'direct whole float', expression: '100.0 * COUNT(*) / (200 * COUNT(*))', expected: 0.5, literal: /CAST\(100 AS DECIMAL\(10, 0\)\)/ },
    { name: 'folded DOUBLE cast', expression: 'CAST(50 + 50 AS DOUBLE) * COUNT(*) / (200 * COUNT(*))', expected: 0.5, literal: /CAST\(100 AS DECIMAL\(10, 0\)\)/ },
    { name: 'folded REAL cast', expression: 'CAST(50 + 50 AS REAL) * COUNT(*) / (200 * COUNT(*))', expected: 0.5, literal: /CAST\(100 AS DECIMAL\(10, 0\)\)/ },
    { name: 'fractional literal', expression: '100.1 * COUNT(*) / (200 * COUNT(*))', expected: 0.5005, literal: /100\.1/ },
    { name: 'integer division', expression: 'COUNT(*) / (2 * COUNT(*))', expected: 0, literal: /\(2 \*/ },
    { name: 'NULL denominator', expression: '100.0 * COUNT(*) / NULLIF(COUNT(*), COUNT(*))', expected: null, literal: /CAST\(100 AS DECIMAL\(10, 0\)\)/ },
    { name: 'decimal ROUND', expression: 'ROUND(1.005 + (COUNT(*) - COUNT(*)), 2)', expected: 1.01, literal: /1\.005/ },
    { name: 'decimal modulo', expression: 'COUNT(*) % 1.5', expected: 0, literal: /1\.5/ },
    { name: 'signed INT minimum division', expression: '-2147483648.0 / COUNT(*)', expected: -2147483648 / 6, literal: /CAST\(-2147483648 AS DECIMAL\(10, 0\)\)/ },
    { name: 'negative precision ROUND', expression: 'ROUND(MAX(500.0), -3)', expected: 1000, literal: /CAST\(500 AS DECIMAL\(10, 0\)\)/ },
    { name: 'large negative precision ROUND', expression: 'ROUND(MAX(500000000.0), -9)', expected: 1000000000, literal: /CAST\(500000000 AS DECIMAL\(10, 0\)\)/ },
    { name: 'CASE ROUND', expression: 'ROUND(MAX(CASE WHEN id > 0 THEN 500.0 ELSE 0.0 END), -3)', expected: 1000, literal: /CAST\(500 AS DECIMAL\(10, 0\)\)/ },
  ];
  let generated: { candidateQueries: SourceQuery[]; controlQueries: SourceQuery[] };

  beforeAll(async () => {
    generated = await compileQueries({
      useNativeSqlPlanner,
      candidateQueries: cases.map(({ expression }) => `SELECT ${expression} AS value ${from}`),
      controlQueries: [
        `SELECT 100.0 * COUNT(*) / (3 * COUNT(*)) AS value ${from}`,
        `SELECT ROUND(MAX(500.0), -3) AS value ${from}`,
        `SELECT ROUND(MAX(500000000.0), -9) AS value ${from}`,
      ],
    });
  });

  it.each(cases)('preserves $name in source SQL', async ({ expression, expected, literal }) => {
    // Execute complete source SQL so local arithmetic cannot hide truncation.
    const query = generated.candidateQueries[cases.findIndex(test => test.expression === expression)];
    expect(query[0]).toMatch(literal);
    const rows = await dbRunner.testQuery(query);
    expect(rows).toHaveLength(1);
    if (expected === null) expect(rows[0].value).toBeNull();
    else expect(Number(rows[0].value)).toBeCloseTo(expected, 6);
  });

  it('reproduces integer truncation while keeping negative precision ROUND valid without the template', async () => {
    const [percentage, smallRound, largeRound] = generated.controlQueries;
    expect(percentage[0]).not.toMatch(/CAST\(100 AS DECIMAL/);
    expect(Number((await dbRunner.testQuery(percentage))[0].value)).toBe(33);
    expect(Number((await dbRunner.testQuery(smallRound))[0].value)).toBe(1000);
    expect(Number((await dbRunner.testQuery(largeRound))[0].value)).toBe(1000000000);
  });
});
