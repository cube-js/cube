import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from './PrepareCompiler';

// Repro for https://github.com/cube-js/cube/issues/11367
//
// A `case` dimension whose `when`/`else` branches reference a multi-stage
// measure that declares `grain: { include: [...] }` should read the grain
// columns off the previous stage's CTE alias. Instead, Tesseract re-emits the
// base cube's column references (e.g. `login_report.school_code`) inside a
// later CTE where only the aggregate stage's alias is in scope, producing SQL
// that fails at the database with an unresolved-column/unrecognized-name error.
describe('Repro #11367 - CASE dimension over a multi-stage measure with grain.include', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('login_report', {
      sql: \`
        SELECT 1 AS ID, 'T1' AS TERRITORY_NAME, 'S1' AS SCHOOL_CODE, TIMESTAMP '2024-01-01' AS DATE_TIME, 'ON1' AS ONBOARDING_UUID, 'L1' AS LOGIN_UUID
        UNION ALL SELECT 2, 'T1', 'S1', TIMESTAMP '2024-01-01', 'ON1', 'L2'
        UNION ALL SELECT 3, 'T1', 'S2', TIMESTAMP '2024-01-02', 'ON2', 'L3'
        UNION ALL SELECT 4, 'T2', 'S3', TIMESTAMP '2024-01-03', 'ON3', 'L4'
      \`,

      dimensions: {
        id: {
          sql: \`ID\`,
          type: \`number\`,
          primaryKey: true,
        },

        territoryName: {
          sql: \`TERRITORY_NAME\`,
          type: \`string\`,
        },

        schoolCode: {
          sql: \`SCHOOL_CODE\`,
          type: \`string\`,
        },

        dateTime: {
          sql: \`DATE_TIME\`,
          type: \`time\`,
        },

        onboardingUuid: {
          sql: \`ONBOARDING_UUID\`,
          type: \`string\`,
        },

        loginUuid: {
          sql: \`LOGIN_UUID\`,
          type: \`string\`,
        },

        percentBucket: {
          type: \`string\`,
          case: {
            when: [
              {
                sql: \`\${loginPercentage} = 0\`,
                label: 'Zero Login',
              },
              {
                sql: \`\${loginPercentage} > 0 AND \${loginPercentage} < 50\`,
                label: 'Below 50',
              },
            ],
            else: { label: 'Above 50' },
          },
        },
      },

      measures: {
        totalLoginCount: {
          sql: \`\${loginUuid}\`,
          type: \`countDistinct\`,
        },

        totalOnboardedCount: {
          sql: \`\${onboardingUuid}\`,
          type: \`countDistinct\`,
        },

        loginPercentage: {
          sql: \`ROUND((\${totalLoginCount} / NULLIF(\${totalOnboardedCount}, 0)) * 100, 2)\`,
          type: \`number\`,
          multiStage: true,
          grain: {
            include: [schoolCode, dateTime.day],
          },
        },

        totalSchool: {
          sql: \`\${schoolCode}\`,
          type: \`countDistinct\`,
        },
      },
    });
  `);

  it('does not reference the base cube alias once it is out of scope', async () => {
    await compiler.compile();
    compiler.throwIfAnyErrors();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['login_report.totalSchool', 'login_report.totalOnboardedCount'],
      dimensions: ['login_report.territoryName', 'login_report.percentBucket'],
      timezone: 'UTC',
      useNativeSqlPlanner: true,
    });

    const [sql] = query.buildSqlAndParams();
    // eslint-disable-next-line no-console
    console.log(sql);

    // The grain columns (school_code, date_time truncated to day) must be
    // read from the previous stage's aggregate alias, not re-derived from the
    // base `login_report` cube alias, which is no longer in scope in the CTE
    // that evaluates the CASE dimension.
    expect(sql).not.toMatch(/\blogin_report\.(school_code|date_time)\b/i);
  });
});
