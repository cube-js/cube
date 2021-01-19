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
});
