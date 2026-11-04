import fs from 'fs';
import path from 'path';

import { prepareYamlCompiler } from './PrepareCompiler';

describe('Switch Dimension', () => {
  it('Switch dimension meta type', async () => {
    const modelContent = fs.readFileSync(
      path.join(process.cwd(), '/test/unit/fixtures/switch-dimension.yml'),
      'utf8'
    );
    const compilers = prepareYamlCompiler(modelContent);
    await compilers.compile();

    const cube = compilers.metaTransformer.cubes[0];
    const numberDim = cube.config.dimensions.find((d) => d.name === 'orders.number');
    const statusDim = cube.config.dimensions.find((d) => d.name === 'orders.status');
    const currencyDim = cube.config.dimensions.find((d) => d.name === 'orders.currency');

    expect(numberDim?.type).toBe('number');
    expect(statusDim?.type).toBe('string');
    expect(currencyDim?.type).toBe('string');
    expect(currencyDim?.values).toEqual(['USD', 'EUR', 'GBP']);
    expect(statusDim).not.toHaveProperty('values');
  });

  describe('values in meta', () => {
    const model = `
cubes:
  - name: orders
    sql: SELECT * FROM orders

    measures:
      - name: count
        type: count

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: status
        sql: status
        type: string

      - name: time_calc
        type: switch
        values:
          - selection
          - last_year
          - ytd
          - cumulative

views:
  - name: orders_all
    cubes:
      - join_path: orders
        includes: "*"

  - name: orders_aliased
    cubes:
      - join_path: orders
        includes:
          - name: time_calc
            alias: calc

  - name: orders_prefixed
    cubes:
      - join_path: orders
        prefix: true
        includes:
          - time_calc
          - status
`;

    let compilers: ReturnType<typeof prepareYamlCompiler>;

    beforeAll(async () => {
      compilers = prepareYamlCompiler(model);
      await compilers.compiler.compile();
    });

    const dimension = (name: string) => {
      const cubeName = name.split('.')[0];
      const cube = compilers.metaTransformer.cubes.find((c) => c.config.name === cubeName);
      return cube?.config.dimensions.find((d) => d.name === name);
    };

    const definition = (cubeName: string, dimensionName: string) => compilers.cubeEvaluator
      .getCubeDefinition(cubeName).dimensions?.[dimensionName];

    const declared = ['selection', 'last_year', 'ytd', 'cumulative'];

    it('emits declared values in declaration order on a cube', () => {
      const timeCalc = dimension('orders.time_calc');

      expect(timeCalc?.type).toBe('string');
      expect(timeCalc?.values).toEqual(declared);
    });

    it('does not emit values on non-switch dimensions', () => {
      expect(dimension('orders.status')).not.toHaveProperty('values');
      expect(dimension('orders.id')).not.toHaveProperty('values');
      expect(dimension('orders_all.status')).not.toHaveProperty('values');
    });

    it.each([
      ['orders_all.time_calc'],
      ['orders_aliased.calc'],
      ['orders_prefixed.orders_time_calc'],
    ])('emits declared values through a view: %s', (name) => {
      const viewDim = dimension(name);

      expect(viewDim?.type).toBe('string');
      expect(viewDim?.values).toEqual(declared);
    });

    it('does not share the values array between members or with the meta output', () => {
      const cubeValues = definition('orders', 'time_calc')?.values;

      expect(definition('orders_all', 'time_calc')?.values).not.toBe(cubeValues);
      expect(definition('orders_aliased', 'calc')?.values).not.toBe(cubeValues);
      expect(dimension('orders.time_calc')?.values).not.toBe(cubeValues);
    });
  });
});
