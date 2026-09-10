/* globals describe, it, expect */
const VerticaQuery = require('../../src/VerticaQuery.js');
const VerticaDriver = require('../../src/VerticaDriver.js');

describe('VerticaQuery templates', () => {
  it('uses valid float cast targets in the runtime dialect', () => {
    const QueryClass = VerticaDriver.dialectClass();
    expect(QueryClass).toBe(VerticaQuery);
    const templates = QueryClass.prototype.sqlTemplates.call(Object.create(QueryClass.prototype));
    expect(templates.types.float).toBe('FLOAT');
    expect(templates.types.double).toBe('DOUBLE PRECISION');
  });
});
