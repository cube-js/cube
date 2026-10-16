import { splitSqlInterval } from '@cubejs-backend/shared';
import { allDialects, dialect } from './allDialects';

// A custom granularity whose origin sits off its unit's boundary is rendered as
// `DATE_TRUNC(unit, x - offset) + offset`, and that offset can carry several units at once —
// `1 year` from April 15 shifts by `3 month 14 day`. Dialects spell such an interval in very
// different ways, and several used to answer one of these shapes with a literal that had
// quietly lost a component.
//
// Only the interval helpers are read here, off a bare prototype rather than a query built on a
// compiled model, since none of them reads instance state on this path.
function query(QueryClass: any): any {
  return Object.create(QueryClass.prototype);
}

const COMPOUND_SHAPES = [
  '3 month 14 day',
  '3 month 6 hour',
  '14 day 6 hour',
  '30 minute 15 second',
  '3 month 14 day 6 hour 30 minute 15 second',
];

const SINGLE_SHAPES = ['3 month', '14 day', '6 hour', '30 minute', '15 second'];

describe('interval rendering across dialects', () => {
  // The invariant, rather than a per-dialect expectation: whatever spelling a dialect picks, a
  // component that went in has to come out. A dialect that drops one produces no error and no
  // wrong-looking SQL — just rows in the wrong bucket.
  it.each(allDialects())('%s keeps every component of an interval', (_name, QueryClass) => {
    const q = query(QueryClass);

    for (const interval of [...SINGLE_SHAPES, ...COMPOUND_SHAPES]) {
      const components = interval.match(/\d+/g) as string[];

      for (const rendered of [q.subtractInterval('x', interval), q.addInterval('x', interval)]) {
        const numbers: string[] = String(rendered).match(/\d+/g) ?? [];

        for (const component of components) {
          // Reported as an object so a failure names the interval, what the dialect made of it
          // and which component went missing, rather than just `false`.
          const lost = numbers.includes(component) ? null : component;
          expect({ interval, rendered, lost }).toEqual({ interval, rendered, lost: null });
        }
      }
    }
  });

  // The dialects that spell a compound interval by hand. Pinned literally, so that a change to
  // one of them shows up as a diff here rather than as a bucket that moved on a warehouse.
  describe.each([
    ['CubeStoreQuery', {
      '3 month': 'DATE_SUB(x, INTERVAL \'3 MONTH\')',
      '3 month 14 day': 'DATE_SUB(x, INTERVAL \'3 MONTH 14 DAY\')',
      '14 day 6 hour': 'DATE_SUB(x, INTERVAL \'14 DAY 6 HOUR\')',
    }],
    ['MysqlQuery', {
      '3 month': 'DATE_SUB(x, INTERVAL 3 MONTH)',
      // MySQL has no month-to-day compound unit, so the units are applied in turn
      '3 month 14 day': 'DATE_SUB(DATE_SUB(x, INTERVAL 3 MONTH), INTERVAL 14 DAY)',
      '14 day 6 hour': 'DATE_SUB(x, INTERVAL \'14 6\' DAY_HOUR)',
    }],
    ['BigqueryQuery', {
      '6 hour': 'TIMESTAMP_SUB(x, INTERVAL 6 HOUR)',
      // A range literal such as `INTERVAL '3 14' MONTH TO DAY` parses on its own, but not as an
      // argument of DATETIME_SUB
      '3 month 14 day':
        'TIMESTAMP_SUB(TIMESTAMP(DATETIME_SUB(DATETIME(x), INTERVAL 3 MONTH)), INTERVAL 14 DAY)',
    }],
    ['ClickHouseQuery', {
      '3 month': 'subDate(x, INTERVAL 3 MONTH)',
      // A sum of intervals of different units is a Tuple that subDate rejects
      '3 month 14 day': 'subDate(subDate(x, INTERVAL 3 MONTH), INTERVAL 14 DAY)',
    }],
    ['HiveQuery', {
      '3 month': '(x - INTERVAL \'3\' month)',
      '3 month 14 day': '((x - INTERVAL \'3\' month) - INTERVAL \'14\' day)',
    }],
    ['PrestodbQuery', {
      '3 month': 'x - interval \'3\' month',
      '3 month 14 day': 'x - interval \'3\' month - interval \'14\' day',
    }],
    ['SnowflakeQuery', {
      '3 month': 'x - interval \'3 month\'',
      // Snowflake separates the components with commas
      '3 month 14 day': 'x - interval \'3 month, 14 day\'',
    }],
    ['SqliteQuery', {
      '3 month': 'strftime(\'%Y-%m-%dT%H:%M:%f\', x, \'-3 month\')',
      // A strftime modifier carries one unit, so each becomes its own argument
      '3 month 14 day': 'strftime(\'%Y-%m-%dT%H:%M:%f\', x, \'-3 month\', \'-14 day\')',
    }],
  ])('%s', (name, expected) => {
    it.each(Object.entries(expected))('subtracts %s', (interval, sql) => {
      expect(query(dialect(name)).subtractInterval('x', interval)).toEqual(sql);
    });
  });

  // Trino and Athena take their interval handling from Presto, so the fix has to reach them too.
  it.each(['TrinoQuery', 'AthenaQuery'])('%s inherits the Presto spelling', name => {
    expect(query(dialect(name)).subtractInterval('x', '3 month 14 day'))
      .toEqual('x - interval \'3\' month - interval \'14\' day');
  });
});

describe('CubeStore DATE_BIN', () => {
  // DATE_BIN takes a month component or a day/time one, never both, unlike DATE_ADD / DATE_SUB.
  it.each(['1 month 15 days', '3 month 3 days 3 hours'])('rejects the mixed interval %s', interval => {
    expect(() => query(dialect('CubeStoreQuery')).dateBin(interval, 'src', '2024-01-01'))
      .toThrow(/Cannot transform interval expression/);
  });

  it.each(['6 months', '1 year', '2 weeks', '15 minutes', '1 week 2 day'])('bins by %s', interval => {
    expect(query(dialect('CubeStoreQuery')).dateBin(interval, 'src', '2024-01-01'))
      .toContain('DATE_BIN(INTERVAL');
  });

  it('has no spelling for a sub-second interval', () => {
    expect(() => query(dialect('CubeStoreQuery')).subtractInterval('x', '5 millisecond'))
      .toThrow(/Cannot transform interval expression/);
  });
});

describe('splitSqlInterval', () => {
  it('splits into one single-unit interval per component', () => {
    expect(splitSqlInterval('3 month 14 day 6 hour')).toEqual(['3 month', '14 day', '6 hour']);
  });

  it('orders coarsest first, whatever order the input used', () => {
    expect(splitSqlInterval('6 hour 3 month 14 day')).toEqual(['3 month', '14 day', '6 hour']);
  });

  it('leaves a single-unit interval alone, sign included', () => {
    expect(splitSqlInterval('3 months')).toEqual(['3 month']);
    expect(splitSqlInterval('-1 day')).toEqual(['-1 day']);
  });

  // Dropping a unit it does not know would hand the dialect a shorter interval than it was
  // given; keeping it lets the dialect reject what it cannot spell.
  it('keeps a unit outside the known set', () => {
    expect(splitSqlInterval('5 millisecond')).toEqual(['5 millisecond']);
    expect(splitSqlInterval('1 day 5 millisecond')).toEqual(['1 day', '5 millisecond']);
  });
});
