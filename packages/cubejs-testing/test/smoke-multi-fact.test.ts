import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, describe, expect, jest, test } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
} from './smoke-tests';

// AOV end to end: the numerator (sales dollars, day/item/location grain) and
// the denominator (distinct transactions, line grain) live in two fact cubes
// that never join to each other, so the whole query is planned as a multi-fact
// one and the ratio is taken after both sides have been aggregated.
//
// The fixture data makes each way of getting it wrong land on a different
// number, so a failure says which invariant broke:
//
//   West  sales 100, transactions 2 (T100 spans three lines)  -> 50
//   East  sales  60, transactions 1 (T201 EXCHANGE, T202 ONLINE) -> 60
//
//   counting lines instead of transactions  -> West 100/4 = 25
//   letting the join multiply the sum       -> West 400/2 = 200
//   dropping the transaction_type filter    -> East  60/2 = 30
//   dropping the channel filter             -> East  60/2 = 30
//
// Multi-fact queries are planned by Tesseract only; the legacy planner cannot
// build a single join tree over two unrelated facts. Nothing here is
// matrix-dependent, so the planner is pinned on in the birdbox env below
// (birdbox spreads `process.env` first, so the pin wins over whatever
// CUBEJS_TESSERACT_SQL_PLANNER the CI leg exports) and the suite runs on
// both legs rather than skipping half of them.
describe('multi-fact derived measure', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'duckdb',
      {
        CUBEJS_DB_TYPE: 'duckdb',
        ...DEFAULT_CONFIG,
        CUBEJS_TESSERACT_SQL_PLANNER: 'true',
      },
      {
        schemaDir: 'multi-fact/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  // Numeric measures come back as strings or numbers depending on the type and
  // the driver, and neither is what these tests are about.
  const byRegion = (rows: any[], key: string) => Object.fromEntries(
    rows.map((row) => [row['RetailAnalysis.region'] ?? row['Locations.region'], Number(row[key])])
  );

  test('each fact is aggregated on its own before the ratio is taken', async () => {
    const result = await client.load({
      measures: ['RetailAnalysis.salesAmount', 'RetailAnalysis.transactionsWithoutReturns'],
      dimensions: ['RetailAnalysis.region'],
    });
    const rows = result.rawData();

    // The line-item side counts transactions, not lines, and the sales side is
    // not multiplied by the number of lines it never joined to.
    expect(byRegion(rows, 'RetailAnalysis.salesAmount')).toEqual({ West: 100, East: 60 });
    expect(byRegion(rows, 'RetailAnalysis.transactionsWithoutReturns')).toEqual({ West: 2, East: 1 });
  });

  test('a view measure divides the two facts', async () => {
    const result = await client.load({
      measures: ['RetailAnalysis.aovBasket'],
      dimensions: ['RetailAnalysis.region'],
    });

    expect(byRegion(result.rawData(), 'RetailAnalysis.aovBasket')).toEqual({ West: 50, East: 60 });
  });

  test('a cube measure divides them the same way', async () => {
    const result = await client.load({
      measures: ['SalesLineItem.aovBasket'],
      dimensions: ['Locations.region'],
    });

    expect(byRegion(result.rawData(), 'SalesLineItem.aovBasket')).toEqual({ West: 50, East: 60 });
  });

  test('the cube-owned measure is reachable through a view', async () => {
    const result = await client.load({
      measures: ['RetailAnalysis.aovBasketFromCube'],
      dimensions: ['RetailAnalysis.region'],
    });

    expect(byRegion(result.rawData(), 'RetailAnalysis.aovBasketFromCube')).toEqual({ West: 50, East: 60 });
  });

  test('the ratio is returned next to its components', async () => {
    const result = await client.load({
      measures: [
        'RetailAnalysis.salesAmount',
        'RetailAnalysis.transactionsWithoutReturns',
        'RetailAnalysis.aovBasket',
      ],
      dimensions: ['RetailAnalysis.region'],
    });
    const rows = result.rawData();

    expect(byRegion(rows, 'RetailAnalysis.aovBasket')).toEqual({ West: 50, East: 60 });
    expect(byRegion(rows, 'RetailAnalysis.salesAmount')).toEqual({ West: 100, East: 60 });
  });

  test('the ratio is taken over the whole result when nothing is grouped', async () => {
    const result = await client.load({ measures: ['RetailAnalysis.aovBasket'] });
    const rows = result.rawData();

    // Asserted before indexing so an empty result reports as a missing row
    // rather than a TypeError.
    expect(rows).toHaveLength(1);
    // 160 dollars over 3 transactions.
    expect(Number(rows[0]['RetailAnalysis.aovBasket'])).toBeCloseTo(160 / 3, 5);
  });
});
