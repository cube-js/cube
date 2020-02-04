/* globals it,describe */
/* eslint-disable quote-props */
const PostgresQuery = require('../adapter/PostgresQuery');
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const { prepareCompiler } = PrepareCompiler;

describe('Extensions', () => {
  const {
    compiler, joinGraph, cubeEvaluator
  } = prepareCompiler(`
    const Funnels = require('Funnels');
    import { dynRef } from 'Reflection';

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
            name: 'Application Installed',
            eventsTable: {
              sql: 'application_installed'
            }
          },
          {
            name: 'ApplicationOpened',
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
      extends: VisitorsFunnel,
      
      measures: {
        conversionsFraction: {
          sql: dynRef('conversions', (c) => \`\${c} / 100.0\`),
          type: 'number'
        }
      }
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
          granularity: 'day',
          dateRange: { from: '2017-01-01', to: '2017-01-30' }
        }],
        timezone: 'America/Los_Angeles'
      });

      console.log(query.buildSqlAndParams()[0]);

      query.buildSqlAndParams()[0].should.match(/application_installed_events/);
      query.buildSqlAndParams()[0].should.match(/application_opened_events/);
      query.buildSqlAndParams()[0].should.match(/user_training_finish_first_events/);
    });

    return result;
  });

  it('dyn ref', () => {
    const result = compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'FooBar.conversionsFraction'
        ],
        timeDimensions: [{
          dimension: 'FooBar.time',
          granularity: 'day',
          dateRange: { from: '2017-01-01', to: '2017-01-30' }
        }],
        timezone: 'America/Los_Angeles'
      });

      console.log(query.buildSqlAndParams()[0]);
    });

    return result;
  });
});
