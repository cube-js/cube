import { allDialects } from './allDialects';

// The SQL API supplies already-rendered joins to statements/select; Tesseract folds
// joins into FROM instead. Guard the loop here because query snapshots do not cover it.
describe('statements/select', () => {
  it.each(allDialects())('%s renders joins after both FROM branches and before WHERE', (_name, QueryClass) => {
    // These templates are defined unconditionally, so no compiled model is needed.
    const { select } = QueryClass.prototype.sqlTemplates.call(Object.create(QueryClass.prototype)).statements;
    const joins = /\{%-?\s*for\s+join\s+in\s+joins\s*-?%\}\s*\{\{-?\s*join\s*-?\}\}\s*\{%-?\s*endfor\s*-?%\}/;
    const fromAlias = /\{\{-?\s*from_alias\s*-?\}\}\s*\{%-?\s*elif\s+from_prepared\s*-?%\}/;
    const fromPrepared = /FROM\s+\{\{-?\s*from_prepared\s*-?\}\}\s*\{%-?\s*endif\s*-?%\}/;
    const filter = /\{%-?\s*if\s+filter\s*-?%\}\s*WHERE\b/;

    expect(select).toMatch(joins);
    expect(select).toMatch(fromAlias);
    expect(select).toMatch(fromPrepared);
    expect(select).toMatch(filter);

    const fromEnd = select.match(fromPrepared)!;
    const joinsEnd = select.match(joins)!;
    expect(select.search(fromPrepared)).toBeGreaterThan(select.search(fromAlias));
    expect(select.search(joins)).toBeGreaterThanOrEqual(fromEnd.index! + fromEnd[0].length);
    expect(select.search(filter)).toBeGreaterThanOrEqual(joinsEnd.index! + joinsEnd[0].length);
  });
});
