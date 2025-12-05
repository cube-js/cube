import { CubeValidator, functionFieldsPatterns } from '../../src/compiler/CubeValidator';
import {
  CubeRefreshKey,
  CubeSymbols,
  PreAggregationDefinitionOriginalSql
} from '../../src/compiler/CubeSymbols';
import { ErrorReporter } from '../../src/compiler/ErrorReporter';

describe('Cube Validation', () => {
  class ConsoleErrorReporter extends ErrorReporter {
    public error(message: any, _e: any) {
      console.log(message);
    }
  }

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
      segments: {
        firstSegment: {
          public: false,
          sql: () => 'test',
        },
        secondSegment: {
          shown: false,
          sql: () => 'test',
        }
      },
      fileName: 'fileName',
    };

    const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
    expect(validationResult.error).toBeFalsy();
  });

  it('cube defined with sql - correct', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => 'SELECT * FROM public.Users',
      fileName: 'fileName',
    };

    const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());

    expect(validationResult.error).toBeFalsy();
  });

  it('cube defined with sqlTable - correct', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sqlTable: () => 'public.Users',
      fileName: 'fileName',
    };

    const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('You must use either sql or sqlTable within a model, but not both');
      }
    } as any);

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
      error: (message: any, _e: any) => {
        console.log(message);
      }
    } as any);

    expect(validationResult.error).toBeFalsy();
  });

  it('view with incorrect included member with alias', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      // it's a hidden field which we use internally
      isView: true,
      fileName: 'fileName',
      cubes: [
        {
          joinPath: () => '',
          prefix: false,
          includes: [
            'member-by-name',
            {
              name: 'member-by-alias',
              alias: 'incorrect Alias'
            }
          ]
        }
      ]
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message: any, _e: any) => {
        console.log(message);
      }
    } as any);

    expect(validationResult.error).toBeTruthy();
  });

  it('view with overridden included members properties', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      // it's a hidden field which we use internally
      isView: true,
      fileName: 'fileName',
      cubes: [
        {
          joinPath: () => '',
          prefix: false,
          includes: [
            'member_by_name',
            {
              name: 'member_by_alias',
              alias: 'correct_alias'
            },
            {
              name: 'member_by_alias_with_overrides',
              title: 'Overridden title',
              description: 'Overridden description',
              format: 'percent',
              meta: {
                f1: 'Overridden 1',
                f2: 'Overridden 2',
              },
            }
          ]
        }
      ]
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message: any, _e: any) => {
        console.log(message);
      }
    } as any);

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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('(refreshKey.every = 12h)');
        expect(message).toContain('does not match regexp');
        expect(message).toContain('CronParser');
      }
    } as any);

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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('unknown timezone');
      }
    } as any);

    expect(validationResult.error).toBeTruthy();
  });

  describe('Measures', () => {
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
        error: (message: any, _e: any) => {
          console.log(message);
          expect(message).toContain('must be one of [count, number,');
        }
      } as any);

      expect(validationResult.error).toBeTruthy();
    });

    it('2 timeShifts, 1 without timeDimension', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => '',
        fileName: 'fileName',
        measures: {
          measure_with_time_shift: {
            multiStage: true,
            type: 'sum',
            sql: () => '',
            timeShift: [
              {
                timeDimension: () => '',
                interval: '1 day',
                type: 'prior',
              },
              {
                interval: '1 day',
                type: 'prior',
              }
            ]
          }
        }
      };

      const validationResult = cubeValidator.validate(cube, {
        error: (message: any, _e: any) => {
          console.log(message);
          expect(message).toContain('(measures.measure_with_time_shift.timeShift[1].timeDimension) is required');
        }
      } as any);

      expect(validationResult.error).toBeTruthy();
    });

    it('3 timeShifts', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => '',
        fileName: 'fileName',
        measures: {
          measure_with_time_shift: {
            multiStage: true,
            type: 'sum',
            sql: () => '',
            timeShift: [
              {
                timeDimension: () => '',
                interval: '1 day',
                type: 'prior',
              },
              {
                timeDimension: () => '',
                interval: '1 year',
                type: 'next',
              },
              {
                timeDimension: () => '',
                interval: '1 week',
                type: 'prior',
              }
            ]
          }
        }
      };

      const validationResult = cubeValidator.validate(cube, {
        error: (message: any, _e: any) => {
          console.log(message);
        }
      } as any);

      expect(validationResult.error).toBeFalsy();
    });

    it('1 timeShift without timeDimension', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => '',
        fileName: 'fileName',
        measures: {
          measure_with_time_shift: {
            multiStage: true,
            type: 'sum',
            sql: () => '',
            timeShift: [
              {
                interval: '1 day',
                type: 'prior',
              }
            ]
          }
        }
      };

      const validationResult = cubeValidator.validate(cube, {
        error: (message: any, _e: any) => {
          console.log(message);
        }
      } as any);

      expect(validationResult.error).toBeFalsy();
    });
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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('timeDimension) is required');
      }
    } as any);

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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('granularity) is required');
        expect(message).toContain('rollups) is required');
      }
    } as any);

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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('granularity) is required');
      }
    } as any);

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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('(preAggregations.eventsByType.scheduledRefresh = true) must be [false]');
      }
    } as any);

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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('number.sql) is required');
        expect(message).toContain('number.columns) is required');
      }
    } as any);

    expect(validationResult.error).toBeTruthy();
  });

  it('preAggregations custom granularities', async () => {
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
          granularity: 'custom_granularity_name',
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
      error: (_message: any, _e: any) => {
        // this callback should not be invoked
        expect(true).toBeFalsy();
      }
    } as any);

    expect(validationResult.error).toBeFalsy();
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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('must be');
      }
    } as any);

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
      error: (message: any, _e: any) => {
        console.log(message);
        expect(message).toContain('are deprecated, please, use');
      }
    } as any);

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
      error: (message: any, _e: any) => {
        // this callback should not be invoked
        expect(true).toBeFalsy();
      }
    } as any);

    expect(validationResult.error).toBeFalsy();
  });

  it('Partition with multi time dimensions', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        eventsByType: {
          type: 'rollup',
          timeDimensions: [
            {
              dimension: () => 'field1',
              granularity: 'day'
            },
            {
              dimension: () => 'field2',
              granularity: 'day'
            }
          ],
          partitionGranularity: 'day',
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message: any, _e: any) => {
        console.log(message);
        // this callback should not be invoked
        expect(true).toBeFalsy();
      }
    } as any);

    expect(validationResult.error).toBeFalsy();
  });

  test('cube - aliases test', async () => {
    const cubeA = {
      name: 'CubeA',
      sql_table: () => 'public.Users',
      public: true,
      refresh_key: {
        sql: () => 'SELECT MAX(created_at) FROM orders',
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
          refreshKey: {
            sql: () => 'SELECT MAX(created_at) FROM orders',
          } satisfies CubeRefreshKey,
        } satisfies PreAggregationDefinitionOriginalSql
      },
      data_source: 'default',
      rewrite_queries: true,
      sql_alias: 'myalias',
      fileName: 'fileName',
    };

    const cubeSymbols = new CubeSymbols();
    cubeSymbols.compile([cubeA], {
      // @ts-ignore
      inContext: () => false,
      error: (message, _e) => {
        console.log(message);
      }
    });

    const cubeValidator = new CubeValidator(cubeSymbols);
    const validationResult = cubeValidator.validate(cubeSymbols.getCubeDefinition('CubeA'), {
      inContext: () => false,
      error: (message: any, _e: any) => {
        console.log(message);
      }
    } as any);

    expect(validationResult.error).toBeFalsy();
  });

  describe('Custom dimension granularities: ', () => {
    const newCube = (granularities) => ({
      name: 'Orders',
      fileName: 'fileName',
      sql: () => 'select * from tbl',
      public: true,
      dimensions: {
        createdAt: {
          public: true,
          sql: () => 'created_at',
          type: 'time',
          granularities
        },
        status: {
          type: 'string',
          sql: () => 'status',
        }
      },
      measures: {
        count: {
          sql: () => 'count',
          type: 'count'
        }
      }
    });

    it('no granularity interval', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = newCube({
        half_year: {}
      });

      const validationResult = cubeValidator.validate(cube, {
        error: (message: any, e: any) => {
          console.log(message);
          expect(message).toContain('(dimensions.createdAt.granularities.half_year.interval) is required');
        }
      } as any);

      expect(validationResult.error).toBeTruthy();
    });

    it('granularity with aligned interval', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      {
        const cube = newCube({
          half_year: {
            interval: '10 years' // useless, but still valid
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '6 months',
            title: 'Half year intervals'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '1 day'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '6 hours'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '15 minutes'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '30 seconds'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }
    });

    it('granularity with aligned interval + offset', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      {
        const cube = newCube({
          half_year: {
            interval: '10 years', // useless, but still valid
            offset: '2 months 3 weeks 4 days',
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '6 months',
            offset: '4 weeks 5 days 6 hours',
            title: 'Half year intervals title'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '1 day',
            offset: '5 days 6 hours 7 minutes',
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '6 hours',
            offset: '5 days 6 hours 7 minutes 8 seconds',
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '15 minutes',
            offset: '1 hours 7 minutes 8 seconds',
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '30 seconds',
            offset: '8 seconds',
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }
    });

    it('granularity with unaligned interval', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());

      {
        const cube = newCube({
          half_year: {
            interval: '5 months',
          }
        });

        const validationResult = cubeValidator.validate(cube, {
          error: (message: any, _e: any) => {
            console.log(message);
            expect(message).toContain('"dimensions.createdAt" does not match any of the allowed types');
            expect(message).toContain('Arbitrary intervals cannot be used without origin point specified');
          }
        } as any);

        expect(validationResult.error).toBeTruthy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '3 quarters',
          }
        });

        const validationResult = cubeValidator.validate(cube, {
          error: (message: any, _e: any) => {
            console.log(message);
            expect(message).toContain('"dimensions.createdAt" does not match any of the allowed types');
            expect(message).toContain('Arbitrary intervals cannot be used without origin point specified');
          }
        } as any);

        expect(validationResult.error).toBeTruthy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '3 weeks',
          }
        });

        const validationResult = cubeValidator.validate(cube, {
          error: (message: any, _e: any) => {
            console.log(message);
            expect(message).toContain('"dimensions.createdAt" does not match any of the allowed types');
            expect(message).toContain('Arbitrary intervals cannot be used without origin point specified');
          }
        } as any);

        expect(validationResult.error).toBeTruthy();
      }

      // Offset doesn't matter in this case
      {
        const cube = newCube({
          half_year: {
            interval: '15 days',
            offset: '1 hours 7 minutes 8 seconds',
            title: 'Just title'
          }
        });

        const validationResult = cubeValidator.validate(cube, {
          error: (message: any, _e: any) => {
            console.log(message);
            expect(message).toContain('"dimensions.createdAt" does not match any of the allowed types');
          }
        } as any);

        expect(validationResult.error).toBeTruthy();
      }
    });

    it('granularity with invalid interval', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = newCube({
        half_year: {
          interval: 'invalid',
        }
      });

      const validationResult = cubeValidator.validate(cube, {
        error: (message: any, _e: any) => {
          console.log(message);
          expect(message).toContain('"dimensions.createdAt" does not match any of the allowed types');
        }
      } as any);

      expect(validationResult.error).toBeTruthy();
    });

    it('granularity with origin + invalid interval', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = newCube({
        half_year: {
          origin: '2024',
          interval: 'invalid',
        }
      });

      const validationResult = cubeValidator.validate(cube, {
        error: (message: any, _e: any) => {
          console.log(message);
          expect(message).toContain('"dimensions.createdAt" does not match any of the allowed types');
        }
      } as any);

      expect(validationResult.error).toBeTruthy();
    });

    it('granularity with invalid origin + interval', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = newCube({
        half_year: {
          origin: 'invalid',
          interval: '3 months',
        }
      });

      const validationResult = cubeValidator.validate(cube, {
        error: (message: any, _e: any) => {
          console.log(message);
          expect(message).toContain('"dimensions.createdAt" does not match any of the allowed types');
        }
      } as any);

      expect(validationResult.error).toBeTruthy();
    });

    it('granularity with origin + interval', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());

      {
        const cube = newCube({
          half_year: {
            interval: '10 years', // useless, but still valid
            origin: '2024',
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '10 months',
            origin: '2024-04',
            title: 'Someone loves number 10'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '2 quarters',
            origin: '2024-04',
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '15 day',
            origin: '2024-05-25',
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '8 hours',
            origin: '2024-09-20 10:00'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '15 minutes',
            origin: '2024-09-20 16:40'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '30 seconds',
            origin: '2024-09-20 16:40:33'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '2 months 30 seconds',
            origin: '2024-09-20T16:40:33.345'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }

      {
        const cube = newCube({
          half_year: {
            interval: '2 months 12 days 14 hours 30 seconds',
            origin: '2024-09-20T16:40:33.345Z'
          }
        });

        const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
        expect(validationResult.error).toBeFalsy();
      }
    });
  });

  describe('Access Policy group/groups support:', () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());

    it('should allow group instead of role', () => {
      const cube = {
        name: 'TestCube',
        fileName: 'test.js',
        sql: () => 'SELECT * FROM test',
        accessPolicy: [{
          group: 'admin',
          rowLevel: { allowAll: true }
        }]
      };

      const result = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(result.error).toBeFalsy();
    });

    it('should allow groups as array', () => {
      const cube = {
        name: 'TestCube',
        fileName: 'test.js',
        sql: () => 'SELECT * FROM test',
        accessPolicy: [{
          groups: ['admin', 'user'],
          rowLevel: { allowAll: true }
        }]
      };

      const result = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(result.error).toBeFalsy();
    });

    it('should allow role as single string (existing behavior)', () => {
      const cube = {
        name: 'TestCube',
        fileName: 'test.js',
        sql: () => 'SELECT * FROM test',
        accessPolicy: [{
          role: 'admin',
          rowLevel: { allowAll: true }
        }]
      };

      const result = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(result.error).toBeFalsy();
    });

    it('should allow group: "*" syntax', () => {
      const cube = {
        name: 'TestCube',
        fileName: 'test.js',
        sql: () => 'SELECT * FROM test',
        accessPolicy: [{
          group: '*',
          rowLevel: { allowAll: true }
        }]
      };

      const result = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(result.error).toBeFalsy();
    });

    it('should reject role and group together', () => {
      const cube = {
        name: 'TestCube',
        fileName: 'test.js',
        sql: () => 'SELECT * FROM test',
        accessPolicy: [{
          role: 'admin',
          group: 'admin',
          rowLevel: { allowAll: true }
        }]
      };

      const result = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(result.error).toBeTruthy();
    });

    it('should reject role and groups together', () => {
      const cube = {
        name: 'TestCube',
        fileName: 'test.js',
        sql: () => 'SELECT * FROM test',
        accessPolicy: [{
          role: 'admin',
          groups: ['user'],
          rowLevel: { allowAll: true }
        }]
      };

      const result = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(result.error).toBeTruthy();
    });

    it('should reject group and groups together', () => {
      const cube = {
        name: 'TestCube',
        fileName: 'test.js',
        sql: () => 'SELECT * FROM test',
        accessPolicy: [{
          group: 'admin',
          groups: ['user'],
          rowLevel: { allowAll: true }
        }]
      };

      const result = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(result.error).toBeTruthy();
    });

    it('should reject access policy without role/group/groups', () => {
      const cube = {
        name: 'TestCube',
        fileName: 'test.js',
        sql: () => 'SELECT * FROM test',
        accessPolicy: [{
          rowLevel: { allowAll: true }
        }]
      };

      const result = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(result.error).toBeTruthy();
    });
  });

  describe('Custom time format for time dimensions (strptime)', () => {
    it('time dimension with valid strptime format - correct', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            format: '%Y-%m-%d'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeFalsy();
    });

    it('time dimension with complex strptime format - correct', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            format: '%d/%m/%Y %H:%M:%S'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeFalsy();
    });

    it('time dimension with literal text in format - correct', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            format: '%Y Year %m Month'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeFalsy();
    });

    it('time dimension with escaped percent - correct', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            format: '%Y-%m-%d %%'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeFalsy();
    });

    it('time dimension with standard format - correct', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            format: 'id'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeFalsy();
    });

    it('time dimension with invalid format (no specifiers) - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            format: 'invalid'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeTruthy();
    });

    it('time dimension with invalid specifier - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            format: '%Y-%K-%d'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeTruthy();
    });

    it('time dimension with incomplete specifier at end - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            format: '%Y-%m-%'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeTruthy();
    });

    it('time dimension with only escaped percent - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            format: '%%'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeTruthy();
    });

    it('non-time dimension with strptime format string - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          status: {
            sql: () => 'status',
            type: 'string',
            format: '%Y-%m-%d'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeTruthy();
    });
  });

  describe('Custom numeric format for measures (d3-format)', () => {
    it('measures with valid d3-format and standard formats - correct', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        measures: {
          // .2f - fixed-point with 2 decimal places
          amount: {
            sql: () => 'amount',
            type: 'sum',
            format: '.2f'
          },
          // ,.0f - thousands separator, no decimals
          revenueGrouped: {
            sql: () => 'revenue',
            type: 'sum',
            format: ',.0f'
          },
          // $,.2f - currency symbol with grouping
          revenueCurrency: {
            sql: () => 'revenue',
            type: 'sum',
            format: '$,.2f'
          },
          // .0% - percentage format
          conversionRate: {
            sql: () => 'conversion_rate',
            type: 'avg',
            format: '.0%'
          },
          // .2s - SI prefix notation (e.g., 1.2k, 3.4M)
          bytes: {
            sql: () => 'bytes',
            type: 'sum',
            format: '.2s'
          },
          // +.2f - always show sign
          change: {
            sql: () => 'change',
            type: 'sum',
            format: '+.2f'
          },
          // 010d - zero-padded integer
          orderId: {
            type: 'count',
            format: '010d'
          },
          // .2~f - trim trailing zeros
          trimmed: {
            sql: () => 'amount',
            type: 'sum',
            format: '.2~f'
          },
          // Standard formats still work
          ratioPercent: {
            sql: () => 'ratio',
            type: 'avg',
            format: 'percent'
          },
          revenueCurrencyStandard: {
            sql: () => 'revenue',
            type: 'sum',
            format: 'currency'
          },
          countNumber: {
            type: 'count',
            format: 'number'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeFalsy();
    });

    it('measure with invalid format (unknown type character) - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        measures: {
          amount: {
            sql: () => 'amount',
            type: 'sum',
            format: '.2z' // 'z' is not a valid d3-format type
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeTruthy();
    });

    it('measure with invalid format (random string) - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        measures: {
          amount: {
            sql: () => 'amount',
            type: 'sum',
            format: 'invalid-format'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeTruthy();
    });
  });

  describe('Custom numeric format for number dimensions (d3-format)', () => {
    it('number dimensions with valid d3-format and standard formats - correct', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          // d3-format specifiers
          price: {
            sql: () => 'price',
            type: 'number',
            format: '.2f'
          },
          quantity: {
            sql: () => 'quantity',
            type: 'number',
            format: ',.0f'
          },
          unitPrice: {
            sql: () => 'unit_price',
            type: 'number',
            format: '$,.2f'
          },
          // Standard dimension formats work for number type
          discount: {
            sql: () => 'discount',
            type: 'number',
            format: 'percent'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeFalsy();
    });

    it('number dimension with invalid format - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          price: {
            sql: () => 'price',
            type: 'number',
            format: 'invalid-format'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeTruthy();
    });

    it('string dimension with d3-format string - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          status: {
            sql: () => 'status',
            type: 'string',
            format: '.2f' // d3-format not allowed for string type
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeTruthy();
    });

    it('dimension with valid order asc - correct', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          status: {
            sql: () => 'status',
            type: 'string',
            order: 'asc'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeFalsy();
    });

    it('dimension with valid order desc - correct', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          createdAt: {
            sql: () => 'created_at',
            type: 'time',
            order: 'desc'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, new ConsoleErrorReporter());
      expect(validationResult.error).toBeFalsy();
    });

    it('dimension with invalid order value - error', async () => {
      const cubeValidator = new CubeValidator(new CubeSymbols());
      const cube = {
        name: 'name',
        sql: () => 'SELECT * FROM public.Orders',
        dimensions: {
          status: {
            sql: () => 'status',
            type: 'string',
            order: 'invalid' // should only accept 'asc' or 'desc'
          },
        },
        fileName: 'fileName',
      };

      const validationResult = cubeValidator.validate(cube, {
        error: (message: any, _e: any) => {
          console.log(message);
          expect(message).toContain('order');
        }
      } as any);

      expect(validationResult.error).toBeTruthy();
    });
  });
});
