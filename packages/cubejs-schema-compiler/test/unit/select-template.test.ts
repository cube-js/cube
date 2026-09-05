import { allDialects, dialect } from './allDialects';

// `statements/select` is what the SQL API renders when it pushes a query down to the data
// source. A dialect that overrides it re-states the whole clause list by hand, so a clause
// dropped from an override is dropped silently: the planner still hands the clause's input
// to the template, the template never reads it, and the omission surfaces only as a syntax
// error at a customer's warehouse.
//
// Only the templates are read here, off a bare prototype rather than a query built on a
// compiled model. That reads the templates a dialect writes unconditionally, which every
// `statements.select` is today; a dialect that gated one on instance state would need a
// real query here, since `this` carries nothing.
function selectTemplate(QueryClass: any): string {
  return QueryClass.prototype.sqlTemplates
    .call(Object.create(QueryClass.prototype))
    .statements
    .select;
}

describe('statements/select', () => {
  it.each(allDialects())('%s renders every join it is given', (_name, QueryClass) => {
    const template = selectTemplate(QueryClass);

    // Each join arrives already rendered by `statements/join`, carrying its own type,
    // source and ON condition, so the select template has only to emit them in order.
    // Looping without emitting is the failure this guards: the projection goes on
    // referencing the right-hand alias while nothing in the FROM clause binds it.
    expect(template).toMatch(/\{%\s*for join in joins\s*%\}/);
    expect(template).toContain('{{ join }}');

    // A join binds an alias the projection reads and the predicates filter on, so it
    // belongs after the FROM branches and ahead of WHERE. Ordering it wrongly is a syntax
    // error in every dialect here, not merely a stylistic difference.
    const joins = template.indexOf('{% for join in joins %}');
    expect(template.indexOf('FROM')).toBeLessThan(joins);
    const where = template.indexOf('WHERE');
    if (where !== -1) {
      expect(joins).toBeLessThan(where);
    }

    // An empty join list has to render nothing at all: the overwhelming majority of
    // pushed-down queries have no joins, and a stray newline or keyword would break them.
    expect(template).not.toMatch(/JOIN\s*\{%\s*for join in joins/);
  });

  it('renders a join between grouped subqueries in T-SQL order', () => {
    // The shape from the report: two grouped CTEs, the second joined to the first and read
    // by the projection. Rendering the template by hand rather than through minijinja
    // keeps this readable; what it pins is the clause order T-SQL requires.
    const template = selectTemplate(dialect('MssqlQuery'));
    const from = template.indexOf('FROM (');
    const joins = template.indexOf('{% for join in joins %}');
    const groupBy = template.indexOf('GROUP BY');
    const offsetFetch = template.indexOf('OFFSET ');

    // FROM ... JOIN ... GROUP BY ... OFFSET/FETCH, in that order and no other
    expect(from).toBeLessThan(joins);
    expect(joins).toBeLessThan(groupBy);
    expect(groupBy).toBeLessThan(offsetFetch);
  });

  it('names the join types the dialects render into that loop', () => {
    // The loop is only worth rendering because the planner can name a join type for it;
    // `join_types` is inherited rather than overridden, so every dialect gets all four.
    const templates = dialect('MssqlQuery').prototype.sqlTemplates
      .call(Object.create(dialect('MssqlQuery').prototype));
    expect(templates.statements.join).toContain('{{ join_type }} JOIN');
    expect(Object.values(templates.join_types)).toEqual(
      expect.arrayContaining(['INNER', 'LEFT', 'RIGHT', 'FULL'])
    );
  });
});
