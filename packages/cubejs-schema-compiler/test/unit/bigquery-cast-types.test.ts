import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';

// The SQL API renders `types` into pushed-down casts, and BigQuery rejects parameterized
// types there ("Parameterized types are not allowed in CAST expressions").
describe('BigQuery cast types', () => {
  const { types } = BigqueryQuery.prototype.sqlTemplates.call(Object.create(BigqueryQuery.prototype));

  it.each(Object.entries(types))('%s names a type without parameters', (_name, template) => {
    expect(template).not.toMatch(/[(<]/);
  });
});
