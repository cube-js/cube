import { dialect } from './allDialects';

function templates(name: string) {
  const QueryClass = dialect(name);
  return QueryClass.prototype.sqlTemplates.call(Object.create(QueryClass.prototype));
}

describe('floating-point literal templates', () => {
  it('limits PostgreSQL numeric-only function handling to PostgreSQL', () => {
    const base = templates('BaseQuery');
    const postgres = templates('PostgresQuery');
    expect(postgres.functions.ROUND).toContain('ROUND(CAST({{ args[0] }} AS NUMERIC), {{ args[1] }})');
    expect(postgres.functions.ROUND).toContain('{% else %}ROUND({{ args_concat }})');
    expect(postgres.operators.float_modulo).toBeUndefined();
    expect(postgres.operators.round_single_arg).toBeUndefined();
    expect(templates('MssqlQuery').operators.float_modulo).toBeUndefined();
    expect(templates('BigqueryQuery').operators.float_modulo).toBeUndefined();
    expect(postgres.expressions.binary).toBe(base.expressions.binary);

    for (const name of ['RedshiftQuery', 'CrateQuery']) {
      const result = templates(name);
      expect(result.functions.ROUND).toBe(base.functions.ROUND);
      expect(result.operators.float_modulo).toBe(name === 'RedshiftQuery' ? undefined : base.operators.float_modulo);
      expect(result.operators.round_single_arg).toBe(base.operators.round_single_arg);
    }
  });

  it.each(['MysqlQuery', 'MongoBiQuery'])('%s supports literals without FLOAT/DOUBLE casts', (name) => {
    const { expressions, operators } = templates(name);
    // The SQL API supplies a round-trippable exponent literal, or none for NULL.
    expect(expressions.float_literal).toBe('{% if value is none %}(NULL + 0e0){% else %}{{ value }}{% endif %}');
    expect(expressions.cast).toBeDefined();
    expect(operators.round_single_arg).toBeUndefined();
    expect(operators.round_multi_arg).toBeUndefined();
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
