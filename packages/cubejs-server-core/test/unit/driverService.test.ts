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

  test('must decorate valid vanila options', async () => {
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
});
