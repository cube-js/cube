import { ClickHouseQuery } from '../../src/adapter/ClickHouseQuery';
import { prepareJsCompiler } from './PrepareCompiler';
import { createCubeSchema } from './utils';

describe('ClickHouseQuery', () => {
  const compilers = prepareJsCompiler(
    createCubeSchema({ name: 'cards', sqlTable: 'card_tbl' })
  );

  it('dateTimeCast uses parseDateTime64BestEffort with precision 3', async () => {
    await compilers.compiler.compile();

    const query = new ClickHouseQuery(compilers, {
      measures: ['cards.count'],
      timeDimensions: [],
      filters: [],
    });

    const result = query.dateTimeCast("'2017-01-01T00:00:00.000'");
    expect(result).toBe("parseDateTime64BestEffort('2017-01-01T00:00:00.000', 3)");
  });

  it('dateTimeCast with timezone uses toDateTime64 with precision 3', async () => {
    await compilers.compiler.compile();

    const query = new ClickHouseQuery(compilers, {
      measures: ['cards.count'],
      timeDimensions: [],
      filters: [],
    });

    const result = query.dateTimeCast("'2017-01-01T00:00:00.000'", 'UTC');
    expect(result).toBe("toDateTime64('2017-01-01T00:00:00.000', 3, 'UTC')");
  });
});
