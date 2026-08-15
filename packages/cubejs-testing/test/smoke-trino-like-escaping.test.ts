import fetch from 'node-fetch';
import { StartedTestContainer } from 'testcontainers';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, describe, expect, it, jest } from '@jest/globals';
import { TrinoDBRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

/**
 * Drives the REST API end to end and checks that `%`, `_` and `\` in a
 * LIKE-based filter value are matched literally rather than as wildcards.
 *
 * Two paths are covered, because they generate the filter SQL with different
 * dialects and only one of them is the Trino dialect:
 *
 *  - `LikeDirect` has no pre-aggregation, so the query runs against Trino. Trino
 *    has no default LIKE escape character, so the value must be escaped AND the
 *    statement must carry an explicit `ESCAPE '\'` clause. This path is correct.
 *  - `LikeRollup` is answered from its pre-aggregation, so the outer query is
 *    generated for Cube Store instead. That path does NOT escape the value: only
 *    PrestodbQuery defines the `filters/like_escape_char` template, and the
 *    native planner skips escaping entirely when it is absent
 *    (rust/cube/cubesqlplanner/.../physical_plan/filter/operators/like.rs). The
 *    wildcard leaks, and the cases below marked `it.failing` document exactly
 *    which filters that breaks. They will start failing - and so need flipping
 *    back to `it` - once the escaping gap is closed.
 */
describe('trino LIKE filter escaping', () => {
  jest.setTimeout(15 * 60 * 1000);

  let db: StartedTestContainer;
  let birdbox: BirdBox;

  beforeAll(async () => {
    db = await TrinoDBRunner.startContainer({});
    birdbox = await getBirdbox(
      'trino',
      {
        CUBEJS_DB_TYPE: 'trino',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(8080)}`,
        CUBEJS_DB_PRESTO_CATALOG: 'memory',
        CUBEJS_DB_USER: 'test',
        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'trino/schema',
      }
    );
  }, 15 * 60 * 1000);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  /**
   * POSTs to the REST API's `/load` endpoint, polling through the
   * "Continue wait" envelope Cube returns while a query - or a
   * pre-aggregation build - is still in flight.
   */
  async function restLoad(query: unknown, attempts = 60): Promise<any> {
    for (let i = 0; i < attempts; i++) {
      const res = await fetch(`${birdbox.configuration.apiUrl}/load`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: DEFAULT_API_TOKEN,
        },
        body: JSON.stringify({ query }),
      });
      const body: any = await res.json();

      if (body.error !== 'Continue wait') {
        if (!res.ok || body.error) {
          throw new Error(`load failed (${res.status}): ${JSON.stringify(body).slice(0, 400)}`);
        }
        return body;
      }
    }
    throw new Error('query did not settle within the allotted attempts');
  }

  const filterQuery = (cube: string, operator: string, value: string) => ({
    measures: [`${cube}.count`],
    filters: [{ member: `${cube}.name`, operator, values: [value] }],
  });

  async function countMatching(cube: string, operator: string, value: string): Promise<number> {
    const body = await restLoad(filterQuery(cube, operator, value));
    return Number(body.data[0][`${cube}.count`]);
  }

  // title, operator, value, expected count out of the 7 fixture rows.
  const CASES: [string, string, string, number][] = [
    ['contains treats % as a literal, not a wildcard', 'contains', '%', 2],
    ['contains treats _ as a literal, not a single-char wildcard', 'contains', '_', 1],
    ['contains treats a backslash as a literal', 'contains', '\\', 1],
    ['notContains treats % as a literal, not a wildcard', 'notContains', '%', 5],
    ['startsWith treats % as a literal, not a wildcard', 'startsWith', '100%', 1],
    ['endsWith treats % as a literal, not a wildcard', 'endsWith', '% off', 1],
    ['still matches values with no special characters', 'contains', 'discount', 2],
  ];

  // On the Cube Store path the value reaches the pattern unescaped. `contains`
  // then matches all 7 rows and `notContains` none of them. The other cases pass
  // only by accident of this fixture - e.g. `startsWith '100%'` becomes `100%%`,
  // which still matches just the one row - so they are NOT evidence that path is
  // sound.
  const ROLLUP_KNOWN_BROKEN = new Set([
    'contains treats % as a literal, not a wildcard',
    'contains treats _ as a literal, not a single-char wildcard',
    'notContains treats % as a literal, not a wildcard',
  ]);

  describe.each([
    ['LikeDirect', 'queried against Trino', new Set<string>()],
    ['LikeRollup', 'served from a pre-aggregation', ROLLUP_KNOWN_BROKEN],
  ])('%s (%s)', (cube, _description, knownBroken) => {
    CASES.forEach(([title, operator, value, expected]) => {
      const runner = knownBroken.has(title) ? it.failing : it;

      runner(title, async () => {
        expect(await countMatching(cube, operator, value)).toBe(expected);
      });
    });
  });

  /** Asks the REST API for the SQL it would run, without running it. */
  async function restSql(query: unknown): Promise<{ text: string; params: unknown[] }> {
    const res = await fetch(`${birdbox.configuration.apiUrl}/sql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: DEFAULT_API_TOKEN,
      },
      body: JSON.stringify({ query }),
    });
    const body: any = await res.json();
    if (!res.ok || body.error) {
      throw new Error(`sql failed (${res.status}): ${JSON.stringify(body).slice(0, 400)}`);
    }
    const [text, params] = body.sql.sql;
    return { text: text.replace(/\s+/g, ' '), params };
  }

  // Guards the premise of the two blocks above: they only test two different
  // escaping paths if the queries really are planned for two different engines.
  // Without this, a change that stopped the rollup being used would leave the
  // Cube Store path silently unchecked while everything still looked green.
  it('the two cubes are planned for different engines', async () => {
    const direct = await restSql(filterQuery('LikeDirect', 'contains', '%'));
    const rollup = await restSql(filterQuery('LikeRollup', 'contains', '%'));

    // Trino: value escaped by the compiler, plus the explicit ESCAPE clause
    // Trino needs because it has no default escape character.
    // eslint-disable-next-line quotes -- double quotes keep the SQL readable
    expect(direct.text).toContain("ESCAPE '\\'");
    expect(direct.params).toEqual(['\\%']);

    // Cube Store: reads the rollup table, and receives the value unescaped -
    // this is the defect the `it.failing` cases above pin down.
    expect(rollup.text).toMatch(/pre_aggregations/);
    expect(rollup.params).toEqual(['%']);
  });
});
