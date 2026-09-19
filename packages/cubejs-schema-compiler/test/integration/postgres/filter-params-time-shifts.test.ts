/* eslint-disable no-template-curly-in-string */
import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// The same ground the Rust tests cover, over the real FILTER_PARAMS recording
// and a real database. Two things only exist here: a callback with a readable
// parameter list compiles to a template of its own — a column form the Rust
// mocks cannot produce — and the answers come from Postgres rather than from
// the shape of the SQL.
//
// The calendar's year is 364 days, so its shifts are a mapping held in its own
// table rather than arithmetic on a date: day n of the calendar carries amount
// n, the fiscal year before it n - 364, two years before n - 728, and a nominal
// `1 year` lands one day off at n - 365.
describe('FILTER_PARAMS addressing a time shift', () => {
  jest.setTimeout(200000);

  const calendarRows = `
    SELECT (DATE '2023-01-01' + (gs.n - 1))::timestamp AS d,
           CASE WHEN gs.n > 364 THEN (DATE '2023-01-01' + (gs.n - 1 - 364))::timestamp END AS d_prev_fy,
           CASE WHEN gs.n > 728 THEN (DATE '2023-01-01' + (gs.n - 1 - 728))::timestamp END AS d_prev_two_fy,
           'FY' || (((gs.n - 1) / 364) + 1) AS fy_name
    FROM generate_series(1, 1092) AS gs(n)
  `;

  const salesRows = `
    SELECT gs.n AS id,
           (DATE '2023-01-01' + (gs.n - 1))::timestamp AS day_d,
           'FY' || (((gs.n - 1) / 364) + 1) AS fy_name,
           gs.n AS amount
    FROM generate_series(1, 1092) AS gs(n)
  `;

  const PLAIN = '${FILTER_PARAMS.fiscal_calendar.reportD.filter(\'day_d\')}';
  const shifted = (name: string, days: number) => '${FILTER_PARAMS.fiscal_calendar.reportD.time_shifts.'
    + `${name}.filter((from, to) => \`day_d >= \${from}::timestamptz - interval '${days} day'`
    + ` AND day_d <= \${to}::timestamptz - interval '${days} day'\`)}`;

  const PREV_FY = shifted('prev_fy', 364);
  const PREV_TWO_FY = shifted('prev_two_fy', 728);
  const PREV_FY_BY_INTERVAL = shifted('prev_fy_by_interval', 364);

  const group = (...bindings: string[]) => `\${FILTER_GROUP(${bindings
    .map(b => b.slice('${'.length, -1))
    .join(', ')})}`;

  // `d` is the primary key the fact table joins to and the shifts move;
  // `reportD` is the label queries group and filter by.
  const modelWith = (predicate: string) => `
    cube('fiscal_calendar', {
      sql: \`${calendarRows}\`,
      calendar: true,
      dimensions: {
        d: {
          sql: \`\${CUBE}.d\`,
          type: 'time',
          primaryKey: true,
          timeShift: [
            { name: 'prev_fy', sql: \`\${CUBE.dPrevFy}\` },
            { name: 'prev_two_fy', sql: \`\${CUBE.dPrevTwoFy}\` },
            { name: 'prev_fy_by_interval', interval: '364 day', type: 'prior' },
          ],
        },
        reportD: {
          sql: \`\${CUBE}.d\`,
          type: 'time',
          timeShift: [
            { name: 'prev_fy', sql: \`\${CUBE.dPrevFy}\` },
            { name: 'prev_two_fy', sql: \`\${CUBE.dPrevTwoFy}\` },
            { name: 'prev_fy_by_interval', interval: '364 day', type: 'prior' },
          ],
        },
        dPrevFy: { sql: \`\${CUBE}.d_prev_fy\`, type: 'time' },
        dPrevTwoFy: { sql: \`\${CUBE}.d_prev_two_fy\`, type: 'time' },
        fyName: { sql: \`\${CUBE}.fy_name\`, type: 'string' },
      },
    });

    cube('sales', {
      sql: \`SELECT * FROM (${salesRows}) AS t WHERE ${predicate}\`,
      joins: {
        fiscal_calendar: {
          sql: \`\${CUBE}.day_d = \${fiscal_calendar.d}\`,
          relationship: 'belongsTo',
        },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        dayD: { sql: 'day_d', type: 'time' },
        fyName: { sql: 'fy_name', type: 'string' },
      },
      measures: {
        amount: { sql: 'amount', type: 'sum' },
        amountPrevFy: {
          type: 'number',
          multiStage: true,
          sql: \`\${amount}\`,
          timeShift: [{ name: 'prev_fy' }],
        },
        amountPrevTwoFy: {
          type: 'number',
          multiStage: true,
          sql: \`\${amount}\`,
          timeShift: [{ name: 'prev_two_fy' }],
        },
        amountPrevFyByInterval: {
          type: 'number',
          multiStage: true,
          sql: \`\${amount}\`,
          timeShift: [{ interval: '364 day', type: 'prior' }],
        },
        amountPrevNominalYear: {
          type: 'number',
          multiStage: true,
          sql: \`\${amount}\`,
          timeShift: [{ interval: '1 year', type: 'prior' }],
        },
      },
      preAggregations: {
        daily: {
          measures: [CUBE.amount],
          timeDimension: CUBE.dayD,
          granularity: 'day',
        },
      },
    });
  `;

  const compilersFor = (predicate: string) => prepareJsCompiler(modelWith(predicate));

  const FULL = group(PLAIN, PREV_FY, PREV_TWO_FY, PREV_FY_BY_INTERVAL);
  const NO_PUSHDOWN = '1 = 1';

  async function rowsFor(predicate: string, query: any) {
    const { compiler, joinGraph, cubeEvaluator } = compilersFor(predicate);
    await compiler.compile();
    const built = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      timezone: 'UTC',
      ...query,
    });
    return dbRunner.testQuery(built.buildSqlAndParams());
  }

  const SHIFTED_QUERY = {
    measures: [
      'sales.amount',
      'sales.amountPrevFy',
      'sales.amountPrevTwoFy',
      'sales.amountPrevFyByInterval',
      'sales.amountPrevNominalYear',
    ],
    timeDimensions: [{
      dimension: 'fiscal_calendar.reportD',
      granularity: 'day',
      dateRange: ['2024-12-29', '2025-01-04'],
    }],
    order: [{ id: 'fiscal_calendar.reportD' }],
  };

  // The model still compiles and answers where addressable shifts are not
  // implemented, which matters because this planner is reached on every request
  // to match and build pre-aggregations. Only the unshifted measure is checked:
  // the plain binding stands everywhere there, shifted stages included.
  it('answers under the legacy planner, where a shift binding states nothing', async () => {
    const rows = await rowsFor(FULL, {
      measures: ['sales.amount'],
      timeDimensions: SHIFTED_QUERY.timeDimensions,
      order: SHIFTED_QUERY.order,
      useNativeSqlPlanner: false,
    });

    expect(rows.map(r => r.sales__amount)).toEqual(
      ['729', '730', '731', '732', '733', '734', '735']
    );
  });

  if (getEnv('nativeSqlPlanner')) {
    // FILTER_PARAMS only narrows a scan, so the model carrying it has to answer
    // exactly what the same model without it answers.
    it('answers what the same model without the pushdown answers', async () => {
      const pushedDown = await rowsFor(FULL, SHIFTED_QUERY);
      const fullScan = await rowsFor(NO_PUSHDOWN, SHIFTED_QUERY);

      expect(pushedDown).toEqual(fullScan);
      // The fiscal year before reads 364 days back, two years before 728, and
      // the nominal year lands a day off — so a stage reading the wrong band
      // shows up as a number here.
      expect(pushedDown).toEqual([
        { fiscal_calendar__report_d_day: '2024-12-29T00:00:00.000Z', sales__amount: '729', sales__amount_prev_fy: '365', sales__amount_prev_two_fy: '1', sales__amount_prev_fy_by_interval: '365', sales__amount_prev_nominal_year: '363' },
        { fiscal_calendar__report_d_day: '2024-12-30T00:00:00.000Z', sales__amount: '730', sales__amount_prev_fy: '366', sales__amount_prev_two_fy: '2', sales__amount_prev_fy_by_interval: '366', sales__amount_prev_nominal_year: '364' },
        { fiscal_calendar__report_d_day: '2024-12-31T00:00:00.000Z', sales__amount: '731', sales__amount_prev_fy: '367', sales__amount_prev_two_fy: '3', sales__amount_prev_fy_by_interval: '367', sales__amount_prev_nominal_year: '365' },
        { fiscal_calendar__report_d_day: '2025-01-01T00:00:00.000Z', sales__amount: '732', sales__amount_prev_fy: '368', sales__amount_prev_two_fy: '4', sales__amount_prev_fy_by_interval: '368', sales__amount_prev_nominal_year: '366' },
        { fiscal_calendar__report_d_day: '2025-01-02T00:00:00.000Z', sales__amount: '733', sales__amount_prev_fy: '369', sales__amount_prev_two_fy: '5', sales__amount_prev_fy_by_interval: '369', sales__amount_prev_nominal_year: '367' },
        { fiscal_calendar__report_d_day: '2025-01-03T00:00:00.000Z', sales__amount: '734', sales__amount_prev_fy: '370', sales__amount_prev_two_fy: '6', sales__amount_prev_fy_by_interval: '370', sales__amount_prev_nominal_year: '368' },
        { fiscal_calendar__report_d_day: '2025-01-04T00:00:00.000Z', sales__amount: '735', sales__amount_prev_fy: '371', sales__amount_prev_two_fy: '7', sales__amount_prev_fy_by_interval: '371', sales__amount_prev_nominal_year: '369' },
      ]);
    });

    // The bug this addresses: the plain binding narrows to the reporting
    // period, which is not where a calendar-shifted stage reads, and every
    // shifted measure came back null. A model binding only the plain column
    // gets no pushdown in those stages, and has to keep answering.
    it('answers with only the plain column bound', async () => {
      const rows = await rowsFor(PLAIN, SHIFTED_QUERY);

      expect(rows).toEqual(await rowsFor(NO_PUSHDOWN, SHIFTED_QUERY));
      expect(JSON.stringify(rows)).not.toContain('null');
    });

    // Every binding of the group names the same member, so position would
    // otherwise decide which one renders.
    it('answers the same whichever order the group binds', async () => {
      const reversed = group(PREV_FY_BY_INTERVAL, PREV_TWO_FY, PREV_FY, PLAIN);

      expect(await rowsFor(reversed, SHIFTED_QUERY)).toEqual(
        await rowsFor(FULL, SHIFTED_QUERY)
      );
    });

    // A calendar shift moves the primary key the fact table joins to, so a
    // binding on any member of that calendar — a fiscal year name here — states
    // nothing in a shifted stage.
    it('answers with a binding on a non-date member of the calendar', async () => {
      const fyNameBinding = '${FILTER_PARAMS.fiscal_calendar.fyName.filter(\'fy_name\')}';
      const withFyName = group(PLAIN, PREV_FY, fyNameBinding);
      const query = {
        measures: ['sales.amount', 'sales.amountPrevFy'],
        timeDimensions: SHIFTED_QUERY.timeDimensions,
        filters: [{ member: 'fiscal_calendar.fyName', operator: 'equals', values: ['FY3'] }],
        order: SHIFTED_QUERY.order,
      };

      expect(await rowsFor(withFyName, query)).toEqual(await rowsFor(NO_PUSHDOWN, query));
    });

    // Nothing filters the member the group binds, so the group states nothing
    // and the scan stays open.
    it('answers with the group bound to a member the query does not filter', async () => {
      const query = {
        measures: ['sales.amount'],
        dimensions: ['sales.fyName'],
        order: [{ id: 'sales.fyName' }],
      };

      expect(await rowsFor(FULL, query)).toEqual(await rowsFor(NO_PUSHDOWN, query));
    });

    // Extracting the group's subtree keeps the query's own structure, so an
    // `or` over the bound member reaches the binding as an `or`.
    it('answers an or filter over the bound member', async () => {
      const query = {
        measures: ['sales.amount'],
        filters: [{
          or: [
            { member: 'fiscal_calendar.reportD', operator: 'inDateRange', values: ['2024-12-29', '2025-01-04'] },
            { member: 'fiscal_calendar.reportD', operator: 'inDateRange', values: ['2023-12-31', '2024-01-06'] },
          ],
        }],
      };

      expect(await rowsFor(FULL, query)).toEqual(await rowsFor(NO_PUSHDOWN, query));
    });

    describe('modelling mistakes', () => {
      const buildFails = async (predicate: string, query: any = SHIFTED_QUERY) => {
        const { compiler, joinGraph, cubeEvaluator } = compilersFor(predicate);
        await compiler.compile();
        const built = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
          timezone: 'UTC',
          ...query,
        });
        return () => built.buildSqlAndParams();
      };

      it('rejects a string column addressing a shift', async () => {
        const predicate = group(
          PLAIN,
          '${FILTER_PARAMS.fiscal_calendar.reportD.time_shifts.prev_fy.filter(\'day_d\')}'
        );

        expect(await buildFails(predicate)).toThrow(/passes a column/);
      });

      it('rejects a binding naming a shift the calendar does not declare', async () => {
        const predicate = group(PLAIN, shifted('no_such_shift', 364));

        expect(await buildFails(predicate)).toThrow(/does not declare/);
      });

      it('rejects a shift binding where the query filters the member by one bound', async () => {
        const query = {
          measures: ['sales.amountPrevFy'],
          dimensions: ['sales.fyName'],
          filters: [{
            member: 'fiscal_calendar.reportD',
            operator: 'beforeDate',
            values: ['2025-01-04'],
          }],
        };

        expect(await buildFails(FULL, query)).toThrow(/needs a date-range filter/);
      });

      it('rejects a measure asking for a shift the calendar does not declare', async () => {
        const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(
          // Declared on both the primary key and the label, so both go.
          modelWith(FULL).split('{ name: \'prev_two_fy\', sql: `${CUBE.dPrevTwoFy}` },').join('')
        );
        await compiler.compile();
        const built = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
          measures: ['sales.amountPrevTwoFy'],
          timeDimensions: SHIFTED_QUERY.timeDimensions,
          timezone: 'UTC',
        });

        expect(() => built.buildSqlAndParams()).toThrow(/Time shift with name prev_two_fy not found/);
      });
    });

    // A build query carries no user filters, so the group states nothing there.
    // Built and read back on the database, so a scan the binding closed off
    // would show up as missing rows.
    it('builds a pre-aggregation over the model and answers from it', async () => {
      const query = {
        measures: ['sales.amount'],
        timeDimensions: [{
          dimension: 'sales.dayD',
          granularity: 'day',
          dateRange: ['2024-12-29', '2025-01-04'],
        }],
        order: [{ id: 'sales.dayD' }],
      };

      const { compiler, joinGraph, cubeEvaluator } = compilersFor(FULL);
      await compiler.compile();
      const built = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        timezone: 'UTC',
        // The rollup is built as a temp table, which takes no schema.
        preAggregationsSchema: '',
        ...query,
      });

      expect(built.preAggregations?.preAggregationsDescription()).toHaveLength(1);

      const rows = await dbRunner.evaluateQueryWithPreAggregations(built);
      expect(rows.map(r => r.sales__amount)).toEqual(
        ['729', '730', '731', '732', '733', '734', '735']
      );
    });
  } else {
    // Addressing a time shift is implemented in the Tesseract planner only.
    test.skip('FILTER_PARAMS addressing a time shift', () => { expect(1).toBe(1); });
  }
});
