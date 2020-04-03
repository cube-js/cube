/* eslint-disable quote-props */
/* globals it,describe */
const SqlParser = require('../parser/SqlParser');
require('should');

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
    sqlParser.isSimpleAsteriskQuery().should.be.deepEqual(true);
    sqlParser.extractWhereConditions('x').should.be.deepEqual(`CAST(
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
    sqlParser.isSimpleAsteriskQuery().should.be.deepEqual(true);
    sqlParser.extractWhereConditions('x').should.be.deepEqual(`CAST(
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
    sqlParser.isSimpleAsteriskQuery().should.be.deepEqual(true);
    sqlParser.extractWhereConditions('x').should.be.deepEqual(`x.a is null`);
  });

  it('do not honor group by', () => {
    const sqlParser = new SqlParser(`select
      asd
    from
      some.ttt
    GROUP BY 1
     `);
    sqlParser.isSimpleAsteriskQuery().should.be.deepEqual(false);
  });

  it('wrapped', () => {
    const sqlParser = new SqlParser(`(select
      *
    from
      some.ttt WHERE 1 = 1)
     `);
    sqlParser.isSimpleAsteriskQuery().should.be.deepEqual(true);
    sqlParser.extractWhereConditions('x').should.be.deepEqual('1 = 1');
  });
});
