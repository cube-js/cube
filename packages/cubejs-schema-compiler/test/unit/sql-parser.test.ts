const { SqlParser } = require('../../src/parser/SqlParser');

describe('SqlParser', () => {
  it('basic where', () => {
    const sqlParser = new SqlParser(`select
      *
    from
      some.ttt
    WHERE
      CAST(
        CONCAT(
          SUBSTRING($1$, 1, 13),
          ':00:00'
        ) AS DATETIME
      ) <= ttt.date_hour
      AND ttt.date_hour <= CAST(
        CONCAT(
          SUBSTRING($2$, 1, 13),
          ':00:00'
        ) AS DATETIME
      )`);
    expect(sqlParser.isSimpleAsteriskQuery()).toEqual(true);
    expect(sqlParser.extractWhereConditions('x')).toEqual(`CAST(
        CONCAT(
          SUBSTRING($1$, 1, 13),
          ':00:00'
        ) AS DATETIME
      ) <= x.date_hour
      AND x.date_hour <= CAST(
        CONCAT(
          SUBSTRING($2$, 1, 13),
          ':00:00'
        ) AS DATETIME
      )`);
  });

  it('and 1 = 1', () => {
    const sqlParser = new SqlParser(`select
      *
    from
      some.ttt
    WHERE
      CAST(
        CONCAT(
          SUBSTRING($1$, 1, 13),
          ':00:00'
        ) AS DATETIME
      ) <= ttt.date_hour
      AND ttt.date_hour <= CAST(
        CONCAT(
          SUBSTRING($2$, 1, 13),
          ':00:00'
        ) AS DATETIME
      ) AND 1 = 1`);
    sqlParser.throwErrorsIfAny();
    expect(sqlParser.isSimpleAsteriskQuery()).toEqual(true);
    expect(sqlParser.extractWhereConditions('x')).toEqual(`CAST(
        CONCAT(
          SUBSTRING($1$, 1, 13),
          ':00:00'
        ) AS DATETIME
      ) <= x.date_hour
      AND x.date_hour <= CAST(
        CONCAT(
          SUBSTRING($2$, 1, 13),
          ':00:00'
        ) AS DATETIME
      ) AND 1 = 1`);
  });

  it('non aliased', () => {
    const sqlParser = new SqlParser(`select
      *
    from
      some.ttt
    WHERE a is null`);
    expect(sqlParser.isSimpleAsteriskQuery()).toEqual(true);
    expect(sqlParser.extractWhereConditions('x')).toEqual('x.a is null');
  });

  it('do not honor group by', () => {
    const sqlParser = new SqlParser(`select
      asd
    from
      some.ttt
    GROUP BY 1
     `);
    expect(sqlParser.isSimpleAsteriskQuery()).toEqual(false);
  });

  it('wrapped', () => {
    const sqlParser = new SqlParser(`(select
      *
    from
      some.ttt WHERE 1 = 1)
     `);
    expect(sqlParser.isSimpleAsteriskQuery()).toEqual(true);
    expect(sqlParser.extractWhereConditions('x')).toEqual('1 = 1');
  });

  it('sql with comment and question mark', () => {
    const sqlParser = new SqlParser('SELECT 1 as test FROM table_name -- this is comment that kaputs Cube -> ?');
    expect(sqlParser.canParse()).toEqual(true);
  });

  it('sql with regex containing question mark', () => {
    const sqlParser = new SqlParser('SELECT * FROM users WHERE name = ? AND REGEXP \'^stripe(?!_direct).{1,}$\'');
    expect(sqlParser.canParse()).toEqual(true);
  });

  it('sql with multiline comment containing question mark', () => {
    const sqlParser = new SqlParser(`SELECT 1 as test FROM table_name 
    /* this is a real
       multiline comment that 
       contains ? character */`);
    expect(sqlParser.canParse()).toEqual(true);
  });

  it('numeric literal in SELECT with table alias extraction', () => {
    const sqlParser = new SqlParser(`SELECT 1 as test_literal, 2.5 as decimal_literal
      FROM users u 
      WHERE u.status = 'active' AND u.created_at > '2024-01-01'`);
    
    expect(sqlParser.canParse()).toEqual(true);
    expect(sqlParser.isSimpleAsteriskQuery()).toEqual(false);
    
    // Verify table alias extraction still works after grammar changes
    const extractedConditions = sqlParser.extractWhereConditions('t');
    expect(extractedConditions).toEqual('t.status = \'active\' AND t.created_at > \'2024-01-01\'');
  });
});
