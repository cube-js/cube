import { CubeValidator } from '../../src/compiler/CubeValidator';
import { CubeSymbols } from '../../src/compiler/CubeSymbols';

describe('Cube Validation', () => {
  it('Alternatives error message', async () => {
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
