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
  RequestContext,
} from '../../src/core/types';
import { CubejsServerCore } from '../../src/core/server';

import * as concurrencyService from '../../src/core/concurrencyService';

describe('Concurrency Service', () => {
  test('must decorate orchestration optins', () => {
    let opt;
  
    // Case 1
    opt = {
      queryCacheOptions: undefined,
      preAggregationsOptions: undefined,
    };
    concurrencyService.decorateOpts(<RequestContext>{}, opt);
    expect(opt.queryCacheOptions).toBeDefined();
    expect(opt.queryCacheOptions.queueOptions).toBeDefined();
    expect(typeof opt.queryCacheOptions.queueOptions).toEqual('function');
    expect(opt.queryCacheOptions.queueOptions()).toBeDefined();
  });
});
