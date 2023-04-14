import { CubeValidator, functionFieldsPatterns } from '../../src/compiler/CubeValidator';
import { CubeSymbols } from '../../src/compiler/CubeSymbols';

describe('Cube Validation', () => {
  it('transpiledFieldsPatterns', async () => {
    const transpiledFieldsPatterns = functionFieldsPatterns()
      .filter((p) => p.indexOf('extends') < 0 && p.indexOf('allDefinitions') < 0)
      .map((p) => {
        p = p.replace(/\./g, '\\.').replace(/\*/g, '[_a-zA-Z][_a-zA-Z0-9]*');
        return RegExp(`^${p}$`);
      });
    transpiledFieldsPatterns.push(/^contextMembers$/);
    transpiledFieldsPatterns.push(/\.sql$/);

    console.log('CubePropContextTranspiler.transpiledFieldsPatterns =', transpiledFieldsPatterns);
  });

  it('cube all ways - correct', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => 'SELECT * FROM public.Users',
      public: true,
      measures: {
        min: {
          public: true,
          sql: () => 'amount',
          type: 'min'
        },
        max: {
          // old way
          shown: true,
          sql: () => 'amount',
          type: 'max'
        },
      },
      dimensions: {
        createdAt: {
          public: true,
          sql: () => 'created_at',
          type: 'time'
        },
        pkey: {
          // old way
          shown: true,
          sql: () => 'id',
          type: 'number',
          primaryKey: true
        },
      },
      fileName: 'fileName',
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
      }
    });

    expect(validationResult.error).toBeFalsy();
  });

  it('cube defined with sql - correct', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => 'SELECT * FROM public.Users',
      fileName: 'fileName',
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
      }
    });

    expect(validationResult.error).toBeFalsy();
  });

  it('cube defined with sqlTable - correct', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sqlTable: () => 'public.Users',
      fileName: 'fileName',
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
      }
    });

    expect(validationResult.error).toBeFalsy();
  });

  it('cube defined with sql and sqlTable - fail', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => 'SELECT * FROM public.Users',
      sqlTable: () => 'public.Users',
      fileName: 'fileName',
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('You must use either sql or sqlTable within a model, but not both');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('view defined by includes - correct', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      // it's a hidden field which we use internally
      isView: true,
      fileName: 'fileName',
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
      }
    });

    expect(validationResult.error).toBeFalsy();
  });

  it('refreshKey alternatives', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      refreshKey: {
        every: '12h',
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('does not match regexp');
        expect(message).toContain('CronParser');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('refreshKey timezones', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      refreshKey: {
        every: '0 * * * *',
        timezone: 'AAA'
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('unknown timezone');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('measures alternatives', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      measures: {
        number: {
          type: 'suma'
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('must be one of [count, number,');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('OriginalSqlSchema', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        eventsByType: {
          type: 'originalSql',
          partitionGranularity: 'day'
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('timeDimension) is required');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('RollUpJoinSchema', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        eventsByType: {
          type: 'rollupJoin',
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('granularity) is required');
        expect(message).toContain('rollups) is required');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('RollUpJoinSchema timeDimension', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        eventsByType: {
          type: 'rollupJoin',
          measures: () => '',
          dimensions: () => '',
          partitionGranularity: 'month',
          timeDimension: () => 'td',
          external: true,
          rollups: () => 0,
          refreshKey: {
            every: '10 minutes',
            updateWindow: '250 day',
            incremental: true
          },
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('granularity) is required');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('RollUpJoinSchema scheduledRefresh', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      fileName: 'filename',
      name: 'name',
      sql: () => '',
      preAggregations: {
        eventsByType: {
          type: 'rollupJoin',
          granularity: 'month',
          scheduledRefresh: true,
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('(preAggregations.eventsByType.scheduledRefresh = true) must be [false]');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('indexes alternatives', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        eventsByType: {
          type: 'originalSql',
          originalSql: () => '',
          indexes: {
            number: {
            }
          }
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('number.sql) is required');
        expect(message).toContain('number.columns) is required');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('preAggregations alternatives', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        distinct: {
          type: 'rollup',
          measureReferences: () => '',
          dimensionReferences: () => '',
          partitionGranularity: 'month',
          granularity: 'days',
          timeDimensionReference: () => '',
          external: true,
          refreshKey: {
            every: '10 minutes',
            updateWindow: '250 day',
            incremental: true
          },
        },
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('must be one of');
        expect(message).not.toContain('rollup) must be');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('preAggregations type unknown', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        eventsByType: {
          type: 'AAA',
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('must be');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('preAggregations deprecated fields', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        eventsByType: {
          rollupReferences: () => '',
          measures: () => '',
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('are deprecated, please, use');
      }
    });

    expect(validationResult.error).toBeTruthy();
  });

  it('No errors', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        eventsByType: {
          type: 'rollup',
          partitionGranularity: 'day',
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        // this callback should not be invoked
        expect(true).toBeFalsy();
      }
    });

    expect(validationResult.error).toBeFalsy();
  });

  test('cube - aliases test', async () => {
    const cubeA = {
      name: 'CubeA',
      sql_table: () => 'public.Users',
      public: true,
      refresh_key: {
        sql: () => `SELECT MAX(created_at) FROM orders`,
      },
      measures: {
        id: {
          sql: () => 'id',
          type: 'count',
          drill_members: () => ['pkey', 'createdAt'],
          rolling_window: {
            trailing: '1 month',
          }
        },
      },
      dimensions: {
        pkey: {
          shown: true,
          sql: () => 'id',
          type: 'number',
          subQuery: true,
          primary_key: true,
          propagate_filters_to_sub_query: true
        },
        createdAt: {
          sql: () => 'created',
          type: 'time',
        },
      },
      pre_aggregations: {
        main: {
          type: 'originalSql',
          time_dimension: () => 'createdAt',
          partition_granularity: 'day',
          refresh_key: {
            sql: () => `SELECT MAX(created_at) FROM orders`,
          },
        }
      },
      data_source: 'default',
      rewrite_queries: true,
      sql_alias: 'myalias',
      fileName: 'fileName',
    };

    const cubeSymbols = new CubeSymbols();
    cubeSymbols.compile([cubeA], {
      inContext: () => false,
      error: (message, _e) => {
        console.log(message);
      }
    });

    const cubeValidator = new CubeValidator(cubeSymbols);
    const validationResult = cubeValidator.validate(cubeSymbols.getCubeDefinition('CubeA'), {
      inContext: () => false,
      error: (message, _e) => {
        console.log(message);
      }
    });

    expect(validationResult.error).toBeFalsy();
  });
});
