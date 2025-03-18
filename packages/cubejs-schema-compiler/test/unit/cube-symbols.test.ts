import * as process from 'node:process';
import { CubeSymbols, CubeDefinition } from '../../src/compiler/CubeSymbols';
import { ErrorReporter } from '../../src/compiler/ErrorReporter';

class ConsoleErrorReporter extends ErrorReporter {
  public error(message: any, _e: any) {
    console.log(message);
  }
}

/**
 * Topological sort in CubeSymbols.compile() should correctly
 * order cubes and views in a way that views depend on cubes will be processed after dependencies
 */
const cubeDefs: CubeDefinition[] = [
  {
    name: 'users_view',
    isView: true,
    cubes: [
      { join_path: (users) => 'users', includes: '*' },
      { joinPath: () => 'users.clients', includes: '*' },
    ]
  },
  {
    name: 'clients',
    measures: {
      Count: { type: 'count', sql: () => 'sql' },
      Sum: { type: 'sum', sql: () => 'sql' },
    },
    dimensions: {
      UserId: { type: 'number', sql: () => 'user_id' },
      Name: { type: 'string', sql: () => 'user_name' },
      CreatedAt: { type: 'time', sql: () => 'created_at' },
    },
  },
  {
    name: 'users',
    measures: {
      count: { type: 'count', sql: () => 'sql' },
      sum: { type: 'sum', sql: () => 'sql' },
    },
    dimensions: {
      userId: { type: 'number', sql: () => 'user_id' },
      name: { type: 'string', sql: () => 'user_name' },
      createdAt: { type: 'time', sql: () => 'created_at' },
    },
    joins: {
      checkins: { relationship: 'hasMany', sql: (CUBE) => `${CUBE}.id = checkins.id` },
      clients: { relationship: 'hasMany', sql: (CUBE) => `${CUBE}.id = clients.id` }
    },
    preAggregations: {
      main: {}
    }
  },
  {
    name: 'view_with_view_as_cube',
    isView: true,
    cubes: [
      { join_path: () => 'emails', includes: '*' },
      { joinPath: () => 'users_view', includes: ['UserId'] },
    ]
  },
  {
    name: 'emails',
    measures: {
      CountMail: { type: 'count', sql: () => 'sql' },
      SumMail: { type: 'sum', sql: () => 'sql' },
    },
    dimensions: {
      mailId: { type: 'number', sql: () => 'user_id' },
      Address: { type: 'string', sql: () => 'email' },
      MailCreatedAt: { type: 'time', sql: () => 'created_at' },
    },
  },
  {
    name: 'checkins',
    measures: {
      CheckinsCount: { type: 'count', sql: () => 'sql' },
      SumCheckins: { type: 'sum', sql: () => 'sql' },
    },
    dimensions: {
      checkinId: { type: 'number', sql: () => 'user_id' },
      CheckinCreatedAt: { type: 'time', sql: () => 'created_at' },
    },
  },

  // Separate graph configuration with loops
  {
    name: 'view',
    isView: true,
    cubes: [
      { join_path: () => 'A', includes: ['aid'] },
    ]
  },
  {
    name: 'A',
    dimensions: { aid: { type: 'number', sql: () => 'aid' } },
    joins: {
      B: { relationship: 'hasMany', sql: (CUBE) => 'join' },
      D: { relationship: 'hasMany', sql: (CUBE) => 'join' }
    },
  },
  {
    name: 'B',
    dimensions: { bid: { type: 'number', sql: () => 'bid' } },
    joins: {
      A: { relationship: 'hasMany', sql: (CUBE) => 'join' },
      E: { relationship: 'hasMany', sql: (CUBE) => 'join' }
    },
  },
  {
    name: 'D',
    dimensions: { did: { type: 'number', sql: () => 'did' } },
    joins: {
      A: { relationship: 'hasMany', sql: (CUBE) => 'join' },
      B: { relationship: 'hasMany', sql: (CUBE) => 'join' },
      E: { relationship: 'hasMany', sql: (CUBE) => 'join' }
    },
  },
  {
    name: 'E',
    dimensions: { eid: { type: 'number', sql: () => 'eid' } },
  },
];

describe('Cube Symbols Compiler', () => {
  it('disallows members of different types with the same name (case sensitive)', () => {
    process.env.CUBEJS_CASE_INSENSITIVE_DUPLICATES_CHECK = 'false';

    const reporter = new ConsoleErrorReporter();
    let compiler = new CubeSymbols();

    let cubeDefsTest: CubeDefinition[] = [
      {
        name: 'users',
        measures: {
          count: { type: 'count', sql: () => 'sql' },
          sum: { type: 'sum', sql: () => 'sql' },
        },
        dimensions: {
          userId: { type: 'number', sql: () => 'user_id' },
          Sum: { type: 'string', sql: () => 'user_name' },
          createdAt: { type: 'time', sql: () => 'created_at' },
        }
      }
    ];

    compiler.compile(cubeDefsTest, reporter);
    reporter.throwIfAny(); // should not throw in this case

    compiler = new CubeSymbols();
    cubeDefsTest = [
      {
        name: 'users',
        measures: {
          count: { type: 'count', sql: () => 'sql' },
          sum: { type: 'sum', sql: () => 'sql' },
        },
        dimensions: {
          userId: { type: 'number', sql: () => 'user_id' },
          sum: { type: 'string', sql: () => 'user_name' },
          createdAt: { type: 'time', sql: () => 'created_at' },
        }
      }
    ];

    compiler.compile(cubeDefsTest, reporter);
    expect(() => reporter.throwIfAny()).toThrow(/sum defined more than once/);
  });

  it('disallows members of different types with the same name (case insensitive)', () => {
    process.env.CUBEJS_CASE_INSENSITIVE_DUPLICATES_CHECK = 'true';

    const reporter = new ConsoleErrorReporter();
    const compiler = new CubeSymbols();

    const cubeDefsTest: CubeDefinition[] = [
      {
        name: 'users',
        measures: {
          count: { type: 'count', sql: () => 'sql' },
          sum: { type: 'sum', sql: () => 'sql' },
        },
        dimensions: {
          userId: { type: 'number', sql: () => 'user_id' },
          Sum: { type: 'string', sql: () => 'user_name' },
          createdAt: { type: 'time', sql: () => 'created_at' },
        }
      }
    ];

    compiler.compile(cubeDefsTest, reporter);
    expect(() => reporter.throwIfAny()).toThrow(/sum defined more than once/);
  });

  it('throws error if dependency loop involving view is detected', () => {
    process.env.CUBEJS_CASE_INSENSITIVE_DUPLICATES_CHECK = 'true';

    const reporter = new ConsoleErrorReporter();
    const compiler = new CubeSymbols(true);

    const cubeDefsTest: CubeDefinition[] = [...cubeDefs];
    // Change the A cube to be a view
    cubeDefsTest[7] = {
      name: 'A',
      isView: true,
      cubes: [
        { join_path: () => 'B', includes: ['bid'] },
        { join_path: () => 'D', includes: ['did'] },
      ]
    };

    expect(() => compiler.compile(cubeDefsTest, reporter)).toThrow(/A view cannot be part of a dependency loop/);
  });

  it('compiles correct cubes and views (case sensitive)', () => {
    process.env.CUBEJS_CASE_INSENSITIVE_DUPLICATES_CHECK = 'false';

    const reporter = new ConsoleErrorReporter();
    let compiler = new CubeSymbols();

    compiler.compile(cubeDefs, reporter);
    reporter.throwIfAny();

    // and with compileViews
    compiler = new CubeSymbols(true);
    compiler.compile(cubeDefs, reporter);
    reporter.throwIfAny();
  });

  it('throws error for duplicates with case insensitive flag', () => {
    process.env.CUBEJS_CASE_INSENSITIVE_DUPLICATES_CHECK = 'true';

    const reporter = new ConsoleErrorReporter();
    let compiler = new CubeSymbols();

    compiler.compile(cubeDefs, reporter);
    reporter.throwIfAny(); // should not throw at this stage

    // and with compileViews
    compiler = new CubeSymbols(true);
    compiler.compile(cubeDefs, reporter);
    expect(() => reporter.throwIfAny()).toThrow(/users_view cube.*conflicts with existing member/);
  });

  it('throws error for including non-existing member in view\'s cube', () => {
    process.env.CUBEJS_CASE_INSENSITIVE_DUPLICATES_CHECK = 'false';

    const reporter = new ConsoleErrorReporter();
    const compiler = new CubeSymbols(true);

    const cubeDefsTest: CubeDefinition[] = [
      {
        name: 'users',
        measures: {
          count: { type: 'count', sql: () => 'sql' },
          sum: { type: 'sum', sql: () => 'sql' },
        },
        dimensions: {
          userId: { type: 'number', sql: () => 'user_id' },
          Sum: { type: 'string', sql: () => 'user_name' },
          createdAt: { type: 'time', sql: () => 'created_at' },
        }
      },
      {
        name: 'users_view',
        isView: true,
        cubes: [
          { join_path: (users) => 'users', includes: ['sum', 'non-existent'] },
        ]
      },
    ];

    compiler.compile(cubeDefsTest, reporter);
    expect(() => reporter.throwIfAny()).toThrow(/Member 'non-existent' is included in 'users_view' but not defined in any cube/);
  });

  it('throws error for using paths in view\'s cube includes members', () => {
    process.env.CUBEJS_CASE_INSENSITIVE_DUPLICATES_CHECK = 'false';

    const reporter = new ConsoleErrorReporter();
    const compiler = new CubeSymbols(true);

    const cubeDefsTest: CubeDefinition[] = [
      {
        name: 'users',
        measures: {
          count: { type: 'count', sql: () => 'sql' },
          sum: { type: 'sum', sql: () => 'sql' },
        },
        dimensions: {
          userId: { type: 'number', sql: () => 'user_id' },
          Sum: { type: 'string', sql: () => 'user_name' },
          createdAt: { type: 'time', sql: () => 'created_at' },
        }
      },
      {
        name: 'users_view',
        isView: true,
        cubes: [
          { join_path: (users) => 'users', includes: ['sum', 'some.other.non-existent'] },
        ]
      },
    ];

    compiler.compile(cubeDefsTest, reporter);
    expect(() => reporter.throwIfAny()).toThrow(/Paths aren't allowed in cube includes but 'some.other.non-existent' provided as include member/);
  });

  it('throws error for using paths in view\'s cube includes members', () => {
    process.env.CUBEJS_CASE_INSENSITIVE_DUPLICATES_CHECK = 'false';

    const reporter = new ConsoleErrorReporter();
    const compiler = new CubeSymbols(true);

    const cubeDefsTest: CubeDefinition[] = [
      {
        name: 'users',
        measures: {
          count: { type: 'count', sql: () => 'sql' },
          sum: { type: 'sum', sql: () => 'sql' },
        },
        dimensions: {
          userId: { type: 'number', sql: () => 'user_id' },
          Sum: { type: 'string', sql: () => 'user_name' },
          createdAt: { type: 'time', sql: () => 'created_at' },
        }
      },
      {
        name: 'users_view',
        isView: true,
        cubes: [
          { join_path: (users) => 'users', includes: '*', excludes: ['some.other.non-existent'] },
        ]
      },
    ];

    compiler.compile(cubeDefsTest, reporter);
    expect(() => reporter.throwIfAny()).toThrow(/Paths aren't allowed in cube excludes but 'some.other.non-existent' provided as exclude member/);
  });
});
