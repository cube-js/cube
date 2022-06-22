/* globals describe, jest, beforeEach, test, expect */

import {
  CreateOptions,
  LoggerFn,
  DbTypeFn,
  DriverFactoryFn,
  DriverContext,
  DatabaseType,
  DriverConfig,
  OrchestratorOptions,
} from '../../src/core/types';
import { CubejsServerCore } from '../../src/core/server';

import * as driverService from '../../src/core/driverService';

let message: string;
let parameters: Record<string, any>;

const logger = (msg: string, params: Record<string, any>) => {
  message = msg;
  parameters = params;
};

describe('Driver Service', () => {
  beforeEach(() => {
    message = '';
    parameters = {};
  });

  test('logger must be set', () => {
    expect(() => {
      driverService.decorateOpts({
        dbType: ((context: DriverContext) => 'postgres'),
      });
    }).toThrow();

    expect(() => {
      driverService.setLogger(logger);
    }).not.toThrow();

    expect(() => {
      driverService.decorateOpts({
        dbType: ((context: DriverContext) => 'postgres'),
      });
    }).not.toThrow();
  });

  test('assertion must work as expected', () => {
    expect(() => {
      driverService.decorateOpts({});
    }).toThrow('Database type missed');

    expect(() => {
      driverService.decorateOpts({
        dbType: ((context: DriverContext) => 'postgres'),
      });
    }).not.toThrow();

    expect(message).toEqual('Cube.js `dbType` Property Deprecation');
  });

  test('must decorate valid vanila create options', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    let opts;
    let dbType;
    let driverFactory;

    // Case 1
    opts = {
      dbType: undefined,
      driverFactory: undefined,
    };
    
    driverService.decorateOpts(<CreateOptions>opts);

    dbType = opts.dbType as DbTypeFn;
    expect(dbType).toBeDefined();
    expect(typeof dbType).toEqual('function');
    expect(dbType({} as DriverContext)).toEqual(process.env.CUBEJS_DB_TYPE);

    driverFactory = opts.driverFactory as DriverFactoryFn;
    expect(driverFactory).toBeDefined();
    expect(typeof driverFactory).toEqual('function');
    expect(driverFactory({} as DriverContext)).toEqual({
      type: process.env.CUBEJS_DB_TYPE,
      options: {},
    });

    // Case 2
    opts = {
      dbType: 'postgres',
      driverFactory: () => CubejsServerCore.createDriver('postgres'),
    };

    driverService.decorateOpts(<CreateOptions>opts);

    dbType = opts.dbType as DbTypeFn;
    expect(dbType).toBeDefined();
    expect(typeof dbType).toEqual('function');
    expect(dbType({} as DriverContext)).toEqual('postgres');

    driverFactory = opts.driverFactory as DriverFactoryFn;
    expect(driverFactory).toBeDefined();
    expect(typeof driverFactory).toEqual('function');
    expect(
      JSON.stringify(driverFactory({} as DriverContext)),
    ).toEqual(
      JSON.stringify(CubejsServerCore.createDriver('postgres')),
    );

    // Case 3
    opts = {
      dbType: () => 'postgres',
      driverFactory: () => CubejsServerCore.createDriver('postgres'),
    };

    driverService.decorateOpts(<CreateOptions>opts);

    dbType = opts.dbType as DbTypeFn;
    expect(dbType).toBeDefined();
    expect(typeof dbType).toEqual('function');
    expect(dbType({} as DriverContext)).toEqual('postgres');

    driverFactory = opts.driverFactory as DriverFactoryFn;
    expect(driverFactory).toBeDefined();
    expect(typeof driverFactory).toEqual('function');
    expect(
      JSON.stringify(driverFactory({} as DriverContext)),
    ).toEqual(
      JSON.stringify(CubejsServerCore.createDriver('postgres')),
    );

    // Case 4
    opts = {
      dbType: () => 'postgres',
      driverFactory: async () => CubejsServerCore.createDriver('postgres'),
    };

    driverService.decorateOpts(<CreateOptions>opts);

    dbType = opts.dbType as DbTypeFn;
    expect(dbType).toBeDefined();
    expect(typeof dbType).toEqual('function');
    expect(dbType({} as DriverContext)).toEqual('postgres');

    driverFactory = opts.driverFactory as DriverFactoryFn;
    expect(driverFactory).toBeDefined();
    expect(typeof driverFactory).toEqual('function');
    expect(
      JSON.stringify(await driverFactory({} as DriverContext)),
    ).toEqual(
      JSON.stringify(CubejsServerCore.createDriver('postgres')),
    );
  });

  test('must decorate valid create options', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    let opts;
    let dbType;
    let driverFactory;

    // Case 1
    opts = {
      dbType: undefined,
      driverFactory: () => ({
        type: process.env.CUBEJS_DB_TYPE,
        options: {},
      }),
    };
    
    driverService.decorateOpts(<CreateOptions>opts);

    dbType = opts.dbType as DbTypeFn;
    expect(dbType).toBeDefined();
    expect(typeof dbType).toEqual('function');
    expect(dbType({} as DriverContext)).toEqual(process.env.CUBEJS_DB_TYPE);

    driverFactory = opts.driverFactory as DriverFactoryFn;
    expect(driverFactory).toBeDefined();
    expect(typeof driverFactory).toEqual('function');
    expect(driverFactory({} as DriverContext)).toEqual({
      type: process.env.CUBEJS_DB_TYPE,
      options: {},
    });

    // Case 2
    opts = {
      dbType: 'postgres',
      driverFactory: () => ({
        type: process.env.CUBEJS_DB_TYPE,
        options: {},
      }),
    };
    
    driverService.decorateOpts(<CreateOptions>opts);

    dbType = opts.dbType as DbTypeFn;
    expect(dbType).toBeDefined();
    expect(typeof dbType).toEqual('function');
    expect(dbType({} as DriverContext)).toEqual('postgres');

    driverFactory = opts.driverFactory as DriverFactoryFn;
    expect(driverFactory).toBeDefined();
    expect(typeof driverFactory).toEqual('function');
    expect(driverFactory({} as DriverContext)).toEqual({
      type: process.env.CUBEJS_DB_TYPE,
      options: {},
    });

    // Case 3
    opts = {
      dbType: 'postgres',
      driverFactory: async () => ({
        type: process.env.CUBEJS_DB_TYPE,
        options: {},
      }),
    };
    
    driverService.decorateOpts(<CreateOptions>opts);

    dbType = opts.dbType as DbTypeFn;
    expect(dbType).toBeDefined();
    expect(typeof dbType).toEqual('function');
    expect(dbType({} as DriverContext)).toEqual('postgres');

    driverFactory = opts.driverFactory as DriverFactoryFn;
    expect(driverFactory).toBeDefined();
    expect(typeof driverFactory).toEqual('function');
    expect(await driverFactory({} as DriverContext)).toEqual({
      type: process.env.CUBEJS_DB_TYPE,
      options: {},
    });
  });

  test('must throw if create options are invalid', () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    let opts;

    // Case 1
    expect(() => {
      opts = {
        dbType: undefined,
        driverFactory: () => true,
      };
      driverService.decorateOpts(<CreateOptions>opts);
      opts.driverFactory();
    }).toThrow('Unexpected driverFactory result value. Must be either DriverConfig or driver instance');

    // Case 2
    expect(() => {
      opts = {
        dbType: undefined,
        driverFactory: 1,
      };
      driverService.decorateOpts(<CreateOptions>opts);
      opts.driverFactory();
    }).toThrow('Unexpected driverFactory type');

    // Case 3
    expect(() => {
      opts = {
        dbType: undefined,
        driverFactory: () => CubejsServerCore.createDriver('postgres'),
      };
      driverService.decorateOpts(<CreateOptions>opts);
      opts.dbType();
    }).toThrow('dbType/driverFactory misconfiguration');

    // Case 4
    expect(() => {
      opts = {
        dbType: undefined,
        driverFactory: async () => ({
          type: process.env.CUBEJS_DB_TYPE,
          options: {},
        }),
      };
      driverService.decorateOpts(<CreateOptions>opts);
      opts.dbType();
    }).toThrow('dbType/driverFactory misconfiguration');

    // Case 5
    expect(() => {
      opts = {
        dbType: true,
        driverFactory: async () => ({
          type: process.env.CUBEJS_DB_TYPE,
          options: {},
        }),
      };
      driverService.decorateOpts(<CreateOptions>opts);
      opts.dbType();
    }).toThrow('Unexpected dbType type');
  });

  test('must calculate driver max pool', () => {
    expect(
      driverService.getDriverMaxPool(<DriverContext>{})
    ).toBeUndefined();

    expect(
      driverService.getDriverMaxPool(
        <DriverContext>{},
        {
          queryCacheOptions: {
            queueOptions: () => ({
              concurrency: 1,
            }),
          },
          preAggregationsOptions: {
            queueOptions: () => ({
              concurrency: 2,
            }),
          },
        },
      )
    ).toEqual(6);

    expect(
      driverService.getDriverMaxPool(
        <DriverContext>{},
        {
          queryCacheOptions: {
            queueOptions: () => ({
              concurrency: 3,
            }),
          },
          preAggregationsOptions: {
            queueOptions: () => ({
              concurrency: 4,
            }),
          },
        },
      )
    ).toEqual(14);
  });

  test('must return driver concurrency value if specified', () => {
    driverService.decorateOpts(<CreateOptions>{
      dbType: undefined,
      driverFactory: () => ({
        type: 'postgres',
        options: {},
      }),
    });
    expect(
      driverService.getDriverConcurrency(<DriverContext>{}),
    ).toEqual(2);

    driverService.decorateOpts(<CreateOptions>{
      dbType: undefined,
      driverFactory: () => ({
        type: 'cubestore',
        options: {},
      }),
    });
    expect(
      driverService.getDriverConcurrency(<DriverContext>{}),
    ).toBeUndefined();
  });

  test('must resolve driver', async () => {
    // Case 1
    driverService.decorateOpts(<CreateOptions>{
      dbType: undefined,
      driverFactory: () => ({
        type: 'postgres',
        options: {},
      }),
    });
    expect(
      JSON.stringify(await driverService.resolveDriver(<DriverContext>{})),
    ).toEqual(
      JSON.stringify(CubejsServerCore.createDriver('postgres')),
    );

    // Case 2
    driverService.decorateOpts(<CreateOptions>{
      dbType: 'postgres',
      driverFactory: () => CubejsServerCore.createDriver('postgres'),
    });
    expect(
      JSON.stringify(await driverService.resolveDriver(<DriverContext>{})),
    ).toEqual(
      JSON.stringify(CubejsServerCore.createDriver('postgres')),
    );

    // Case 3
    driverService.decorateOpts(<CreateOptions>{
      dbType: 'postgres',
      driverFactory: async () => CubejsServerCore.createDriver('postgres'),
    });
    expect(
      JSON.stringify(await driverService.resolveDriver(<DriverContext>{})),
    ).toEqual(
      JSON.stringify(CubejsServerCore.createDriver('postgres')),
    );
  });
});
