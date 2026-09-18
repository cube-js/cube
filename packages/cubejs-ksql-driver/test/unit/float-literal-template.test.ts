import { KsqlDriver } from '../../src/KsqlDriver';

describe('ksqlDB floating-point templates', () => {
  it('disables Float32 pushdown while retaining Double support', () => {
    const QueryClass = KsqlDriver.dialectClass();
    const templates = QueryClass.prototype.sqlTemplates.call(Object.create(QueryClass.prototype));
    expect(templates.types.float).toBeUndefined();
    expect(templates.types.double).toBe('DOUBLE');
    expect(templates.expressions.float_literal).toBeUndefined();
  });
});
