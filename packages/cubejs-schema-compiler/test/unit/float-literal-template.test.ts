import { dialect } from './allDialects';

function templates(name: string) {
  const QueryClass = dialect(name);
  return QueryClass.prototype.sqlTemplates.call(Object.create(QueryClass.prototype));
}

describe('floating-point literal templates', () => {
  it.each(['MysqlQuery', 'MongoBiQuery'])('%s supports literals without FLOAT/DOUBLE casts', (name) => {
    const { expressions } = templates(name);
    // The SQL API supplies a round-trippable exponent literal, or none for NULL.
    expect(expressions.float_literal).toBe('{% if value is none %}(NULL + 0e0){% else %}{{ value }}{% endif %}');
    expect(expressions.cast).toBe('CAST({{ expr }} AS {{ data_type }})');
  });

  it.each([
    ['OracleQuery', 'BINARY_FLOAT', 'BINARY_DOUBLE'],
    ['VerticaQuery', 'FLOAT', 'DOUBLE PRECISION'],
    ['MssqlQuery', 'FLOAT(24)', 'FLOAT(53)'],
    ['PostgresQuery', 'REAL', 'DOUBLE PRECISION'],
  ])('%s uses valid floating-point cast targets', (name, float, double) => {
    const result = templates(name);
    expect(result.types.float).toBe(float);
    expect(result.types.double).toBe(double);
    expect(result.expressions.float_literal).toBeUndefined();
  });
});
