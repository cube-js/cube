import { allDialects, dialect } from './allDialects';

// `statements/union` is read by the SQL API when it pushes a set operation down to the
// data source, and by nothing else, so these assertions are what stands between an edit
// to one of these templates and a syntax error at a customer's warehouse.
//
// Only the templates are read here, off a bare prototype rather than a query built on a
// compiled model. That reads the templates a dialect writes unconditionally, which every
// `statements.union` is today; a dialect that gated one on instance state would need a
// real query here, since `this` carries nothing.
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

    // The operator itself: without it the rendered SQL is not a set operation at all
    expect(template).toMatch(/\bUNION\b/);
    // Every query of the operation, rendered rather than merely looped over
    expect(template).toMatch(/\{%\s*for query in queries\s*%\}/);
    expect(template).toContain('{{ query');
    // The row cap bounds the result of the whole operation, and is interpolated into the
    // clause that bounds it rather than only guarding it
    expect(template).toMatch(/\{%\s*if limit is not none\s*%\}[^]*\{\{\s*limit\s*\}\}/);
    // However the dialect spells the operator, `ALL` is what keeps the duplicates, so
    // every `ALL` sits on the branch that keeps them and nowhere else. The dialects write
    // that branch two ways: naming only `ALL`, or naming both modes.
    const keepsDuplicates = /\{%\s*if not distinct\s*%\}ALL|\{%\s*if distinct\s*%\}DISTINCT\{%\s*else\s*%\}ALL/;
    expect(template).toMatch(keepsDuplicates);
    expect(template.replace(new RegExp(keepsDuplicates.source, 'g'), '')).not.toContain('ALL');
    // Balanced tags: an unclosed block renders as a template error at query time
    expect((template.match(/\{%\s*if\b/g) || []).length)
      .toEqual((template.match(/\{%\s*endif\s*%\}/g) || []).length);
    expect((template.match(/\{%\s*for\b/g) || []).length)
      .toEqual((template.match(/\{%\s*endfor\s*%\}/g) || []).length);
  });

  it('leaves the template undefined where a set operation cannot be expressed', () => {
    // A compound select takes no parenthesised operand in SQLite
    expect(unionTemplate(dialect('SqliteQuery'))).toBeUndefined();
  });

  it('names the mode of the operation where the dialect requires it', () => {
    // GoogleSQL and ClickHouse both reject a bare UNION
    for (const name of ['BigqueryQuery', 'ClickHouseQuery']) {
      expect(unionTemplate(dialect(name))).toContain('{% if distinct %}DISTINCT{% else %}ALL{% endif %}');
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
      expect(unionTemplate(dialect(name))).toContain(clause);
    }
  });
});
