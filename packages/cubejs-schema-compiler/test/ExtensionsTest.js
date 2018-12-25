const CompileError = require('../compiler/CompileError');
const PostgresQuery = require('../adapter/PostgresQuery');
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const prepareCompiler = PrepareCompiler.prepareCompiler;

describe('Extensions', () => {
  const { compiler, joinGraph, cubeEvaluator, transformer } = prepareCompiler(`
    const Funnels = require('Funnels');

    cube(\`VisitorsFunnel\`, {
      extends: Funnels.eventFunnel({
        userId: {
          sql: 'anonymous_id'
        },
        time: {
          sql: 'timestamp'
        },
        steps: [
          {
            name: 'application_installed',
            eventsTable: {
              sql: 'application_installed'
            }
          },
          {
            name: 'application_opened',
            eventsView: {
              sql: 'application_opened'
            },
            timeToConvert: '1 day',
            nextStepUserId: {
              sql: 'auth_id'
            }
          },
          {
            name: 'user_training_finish_first',
            eventsCube: {
              sql: 'user_training_finish_first'
            },
            timeToConvert: '1 day',
            userId: {
              sql: 'auth_id'
            }
          }
        ]
      })
    })

    cube(\`FooBar\`, {
      extends: VisitorsFunnel
    })
    `);

  it('funnel', () => {
    const result = compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'VisitorsFunnel.conversions'
        ],
        timeDimensions: [{
          dimension: 'VisitorsFunnel.time',
          granularity: 'date',
          dateRange: { from: '2017-01-01', to: '2017-01-30' }
        }],
        timezone: 'America/Los_Angeles'
      });

      console.log(query.buildSqlAndParams()[0]);
    });

    return result;
  });
});
