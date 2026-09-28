import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Extensions', () => {
  jest.setTimeout(200 * 1000);

  // A three-step funnel (View -> Cart -> Purchase). Event sources are inline
  // `VALUES` selects so the test is fully self-contained and needs no fixture.
  //
  // Raw events (user_id, ts):
  //   View:     1@01-01, 2@01-02, 3@01-03, 4@01-04
  //   Cart:     1@01-03, 2@01-20, 3@2016-12-31, 4@01-06   (timeToConvert: 5 day)
  //   Purchase: 1@01-10
  //
  // Cart has `timeToConvert: '5 day'`, so a Cart event only counts when it
  // happens after the View event and within 5 days of it:
  //   user 1: cart 01-03 is 2 days after view 01-01  -> converts
  //   user 2: cart 01-20 is 18 days after view 01-02 -> dropped (too late)
  //   user 3: cart 2016-12-31 is before view 01-03   -> dropped (before)
  //   user 4: cart 01-06 is 2 days after view 01-04  -> converts
  // Purchase joins off Cart, so only users who reached Cart can reach it:
  //   user 1 purchases, user 4 does not.
  //
  // Expected conversions per step: View=4, Cart=2, Purchase=1.
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    const Funnels = require('Funnels');

    cube('Funnel', {
      extends: Funnels.eventFunnel({
        userId: { sql: 'user_id' },
        time: { sql: 'ts' },
        steps: [
          {
            name: 'View',
            eventsTable: {
              sql: "select user_id, ts from (values (1, timestamp '2017-01-01'), (2, timestamp '2017-01-02'), (3, timestamp '2017-01-03'), (4, timestamp '2017-01-04')) as t(user_id, ts)"
            }
          },
          {
            name: 'Cart',
            timeToConvert: '5 day',
            eventsTable: {
              sql: "select user_id, ts from (values (1, timestamp '2017-01-03'), (2, timestamp '2017-01-20'), (3, timestamp '2016-12-31'), (4, timestamp '2017-01-06')) as t(user_id, ts)"
            }
          },
          {
            name: 'Purchase',
            eventsTable: {
              sql: "select user_id, ts from (values (1, timestamp '2017-01-10')) as t(user_id, ts)"
            }
          }
        ]
      })
    })
  `, { allowNodeRequire: true });

  async function runQueryTest(q: any, expectedResult: any) {
    await compiler.compile();
    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      { ...q, timezone: 'UTC', preAggregationsSchema: '' }
    );

    const qp = query.buildSqlAndParams();
    console.log(qp);

    const res = await dbRunner.testQuery(qp);
    console.log(JSON.stringify(res));

    expect(res).toEqual(expectedResult);
  }

  describe('Funnels', () => {
    it('conversions per step', async () => runQueryTest({
      measures: ['Funnel.conversions'],
      dimensions: ['Funnel.step'],
      order: [{ id: 'Funnel.conversions', desc: true }],
    }, [
      { funnel__step: 'View', funnel__conversions: '4' },
      { funnel__step: 'Cart', funnel__conversions: '2' },
      { funnel__step: 'Purchase', funnel__conversions: '1' },
    ]));

    it('total conversions across all steps', async () => runQueryTest({
      measures: ['Funnel.conversions'],
    }, [
      { funnel__conversions: '7' },
    ]));
  });
});
