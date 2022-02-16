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
        expect(message).toContain('must be one of [count, number, sum');
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
});
