import { CubeValidator } from '../../src/compiler/CubeValidator';
import { CubeSymbols } from '../../src/compiler/CubeSymbols';

describe('Cube Validation', () => {
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
        timezone: 'Timezone'
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
          sql: () => '',
          type: 'suma'
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

  it('indexes alternatives', async () => {
    const cubeValidator = new CubeValidator(new CubeSymbols());
    const cube = {
      name: 'name',
      sql: () => '',
      fileName: 'fileName',
      preAggregations: {
        eventsByType: {
          type: 'originalSql',
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
        expect(message).toContain('number.sql = undefined) is required');
        expect(message).toContain('number.columns = undefined) is required');
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
        eventsByType: {
          type: 'rollup',
          partitionGranularity: 'days',
        }
      }
    };

    const validationResult = cubeValidator.validate(cube, {
      error: (message, e) => {
        console.log(message);
        expect(message).toContain('Possible reasons');
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
          type: 'unknown',
          partitionGranularity: 'day',
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
