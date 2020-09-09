/* globals it, describe, after */
/* eslint-disable quote-props */
const UserError = require('../compiler/UserError');
const PostgresQuery = require('../adapter/PostgresQuery'); 
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const { prepareCompiler } = PrepareCompiler;
const dbRunner = require('./DbRunner');

describe('SQL Generation', function test() {
  this.timeout(90000);

  after(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator, transformer } = prepareCompiler(` 
    cube('cards', {
      sql: \`
      select * from cards
      \`,
 
      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        }
      }
    }) 
    `);

    
  it('Test for everyRefreshKeySql', () => {
    const result = compiler.compile().then(() => {
      let query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [],
        filters: [  ],
        timezoneOffset: '-08:00'
      });

      let r;
      r = query.everyRefreshKeySql({
        every: '1 hour'
      })
      r.should.be.equal("FLOOR((EXTRACT(EPOCH FROM NOW())) / 3600)")

      r = query.everyRefreshKeySql({
        every: '0 * * * * *',
        timezoneOffset: '-08:00'
      })
      r.should.be.equal("FLOOR((-25200 + 0 + EXTRACT(EPOCH FROM NOW())) / 60)") 

      r = query.everyRefreshKeySql({
        every: '0 * * * *',
        timezoneOffset: '-08:00'
      })
      r.should.be.equal("FLOOR((-25200 + 0 + EXTRACT(EPOCH FROM NOW())) / 3600)") 

      r = query.everyRefreshKeySql({
        every: '30 * * * *',
        timezoneOffset: '-08:00'
      })
      r.should.be.equal("FLOOR((-25200 + 1800 + EXTRACT(EPOCH FROM NOW())) / 3600)") 

      r = query.everyRefreshKeySql({
        every: '30 5 * * 5',
        timezoneOffset: '-08:00'
      })
      r.should.be.equal("FLOOR((-25200 + 394200 + EXTRACT(EPOCH FROM NOW())) / 604800)") 

      for(let i = 1; i < 59; i++)
      { 
        r = query.everyRefreshKeySql({
          every: `${i} * * * *`,
          timezoneOffset: '-08:00'
        }) 
        console.log(r, i, `${i} * * * *`)
        r.should.be.equal(`FLOOR((-25200 + ${i*60} + EXTRACT(EPOCH FROM NOW())) / ${1*60*60})`)
      }

      try{
        r = query.everyRefreshKeySql({
          every: '*/9 */7 * * *',
          timezoneOffset: '-08:00'
        })
        
        throw new Error();
      }catch(error){ 
        error.should.be.instanceof(UserError);
      }
      
      try{
        r = query.everyRefreshKeySql({
          every: '6 * * * *',
          timezoneOffset: 'America/Los_Angeles'
        })
        
        throw new Error();
      }catch(error){ 
        error.should.be.instanceof(UserError);
      }
      
    });

    return result;
  });
 
});