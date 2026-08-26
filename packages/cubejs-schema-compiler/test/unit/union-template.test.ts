import fs from 'fs';
import path from 'path';

// Read off the directory rather than listed by hand, so a dialect added later cannot
// quietly escape the invariants below.
const ADAPTER_DIR = path.join(__dirname, '..', '..', 'src', 'adapter');

function allDialects(): [string, any][] {
  const classes = fs.readdirSync(ADAPTER_DIR)
    // Tests run from `dist`, so the adapter dir holds `.js`; `.ts` keeps this working if
    // they are ever run from source.
    .map(file => file.match(/^(\w+Query)\.(?:ts|js)$/)?.[1])
    .filter((name): name is string => !!name && name !== 'BaseQuery')
    // eslint-disable-next-line global-require, import/no-dynamic-require
    .map(name => [name, require(path.join(ADAPTER_DIR, name))[name]] as [string, any]);

  expect(classes.length).toBeGreaterThan(10);
  return classes;
}

// `statements/union` is read by the SQL API when it pushes a set operation down to the
// data source, and by nothing else, so these assertions are what stands between an edit
// to one of these templates and a syntax error at a customer's warehouse.
//
// Only the templates are read here: a Query needs no compiled model to answer with them.
function unionTemplate(QueryClass: any): string | undefined {
  return QueryClass.prototype.sqlTemplates
    .call(Object.create(QueryClass.prototype))
    .statements
    .union;
}

describe('statements/union', () => {
  it.each(allDialects())('%s renders every query, once, bounded', (_name, QueryClass) => {
    const template = unionTemplate(QueryClass);
    if (template === undefined) {
      // Deliberately opted out: with no template the SQL API leaves the set operation to
      // post processing rather than pushing down SQL the data source cannot parse
      return;
    }

    // Every query of the operation, and only the ones it was given
    expect(template).toContain('{% for query in queries %}');
    // The row cap bounds the result of the whole operation
    expect(template).toMatch(/\{%\s*if limit is not none\s*%\}/);
    // However the dialect spells the operator, `ALL` is what keeps the duplicates, so it
    // belongs to the branch that keeps them and to no other
    expect(template).toMatch(/UNION.*ALL/s);
    expect(template).toMatch(/\{%\s*if (not )?distinct\s*%\}/);
    // Balanced tags: an unclosed block renders as a template error at query time
    expect((template.match(/\{%\s*if /g) || []).length)
      .toEqual((template.match(/\{%\s*endif\s*%\}/g) || []).length);
    expect((template.match(/\{%\s*for /g) || []).length)
      .toEqual((template.match(/\{%\s*endfor\s*%\}/g) || []).length);
  });

  it('leaves the template undefined where a set operation cannot be expressed', () => {
    // A compound select takes no parenthesised operand in SQLite
    // eslint-disable-next-line global-require, import/no-dynamic-require
    const { SqliteQuery } = require(path.join(ADAPTER_DIR, 'SqliteQuery'));
    expect(unionTemplate(SqliteQuery)).toBeUndefined();
  });

  it('names the mode of the operation where the dialect requires it', () => {
    // GoogleSQL and ClickHouse both reject a bare UNION
    for (const name of ['BigqueryQuery', 'ClickHouseQuery']) {
      // eslint-disable-next-line global-require, import/no-dynamic-require
      const QueryClass = require(path.join(ADAPTER_DIR, name))[name];
      expect(unionTemplate(QueryClass)).toContain('{% if distinct %}DISTINCT{% else %}ALL{% endif %}');
    }
  });

  it('bounds the operation with the row limiting clause of the dialect', () => {
    const clauses: [string, string][] = [
      ['PostgresQuery', 'LIMIT {{ limit }}'],
      ['OracleQuery', 'FETCH NEXT {{ limit }} ROWS ONLY'],
      // Neither TOP nor OFFSET/FETCH attaches to a compound query in T-SQL
      ['MssqlQuery', 'SELECT TOP {{ limit }} * FROM ('],
    ];
    for (const [name, clause] of clauses) {
      // eslint-disable-next-line global-require, import/no-dynamic-require
      const QueryClass = require(path.join(ADAPTER_DIR, name))[name];
      expect(unionTemplate(QueryClass)).toContain(clause);
    }
  });
});
