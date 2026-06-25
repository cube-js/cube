import {
  bridgeHarnessAvailable,
  describeModel,
  prepareModelRaw as prepareModel,
} from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

/**
 * Minimal stand-in for the production `SchemaSource` (from
 * cubejs-schema-compiler). The Rust side only consumes `primaryKeys`
 * and `cubes()`; each cube shape mirrors what the real
 * `SchemaSource.cubes()` wrapper exposes after prepareCompiler runs.
 */
function makeSchemaSource(cubes: any[], primaryKeys: Record<string, string[]> = {}) {
  return {
    primaryKeys,
    cubes: () => cubes,
  };
}

function makeCube(overrides: Partial<any> = {}): any {
  return {
    name: 'Users',
    sqlAlias: undefined,
    isView: false,
    calendar: false,
    measures: [],
    dimensions: [],
    segments: [],
    joins: [],
    preAggregations: [],
    accessPolicy: [],
    includedMembers: [],
    ...overrides,
  };
}

describeBridge('bridge: model roundtrip via prepareModel / __testBridgeModelDescribe', () => {
  it('returns the cubes in the model with member counts', () => {
    const source = makeSchemaSource(
      [
        makeCube({
          name: 'Users',
          measures: [
            { name: 'count', type: 'count', ownedByCube: true },
            { name: 'total', type: 'sum', ownedByCube: true },
          ],
          dimensions: [
            { name: 'id', type: 'number', primaryKey: true, ownedByCube: true },
            { name: 'status', type: 'string', ownedByCube: true },
          ],
        }),
        makeCube({
          name: 'Orders',
          measures: [{ name: 'count', type: 'count', ownedByCube: true }],
          dimensions: [{ name: 'id', type: 'number', primaryKey: true, ownedByCube: true }],
        }),
      ],
      {
        Users: ['id'],
        Orders: ['id'],
      }
    );

    const handle = prepareModel(source);
    const view = describeModel(handle);

    // SchemaModelBuilder iterates by insertion order from JS, but the
    // describe helper sorts alphabetically.
    expect(view.cubes.map(c => c.name)).toEqual(['Orders', 'Users']);

    const users = view.cubes.find(c => c.name === 'Users')!;
    expect(users.measure_count).toBe(2);
    expect(users.dimension_count).toBe(2);
    expect(users.is_view).toBe(false);

    const orders = view.cubes.find(c => c.name === 'Orders')!;
    expect(orders.measure_count).toBe(1);
    expect(orders.dimension_count).toBe(1);
  });

  it('the handle survives multiple describe calls', () => {
    const source = makeSchemaSource(
      [makeCube({ name: 'Users', measures: [{ name: 'count', type: 'count' }] })],
      { Users: [] }
    );
    const handle = prepareModel(source);
    for (let i = 0; i < 50; i += 1) {
      const view = describeModel(handle);
      expect(view.cubes).toHaveLength(1);
      expect(view.cubes[0].measure_count).toBe(1);
    }
  });

  it('different handles refer to independent models', () => {
    const a = prepareModel(
      makeSchemaSource([makeCube({ name: 'A' })])
    );
    const b = prepareModel(
      makeSchemaSource([makeCube({ name: 'B' }), makeCube({ name: 'C' })])
    );

    expect(describeModel(a).cubes.map(c => c.name)).toEqual(['A']);
    expect(describeModel(b).cubes.map(c => c.name)).toEqual(['B', 'C']);
  });

  it('describeModel rejects non-RustBox arguments and wrong-type boxes', () => {
    expect(() => describeModel({})).toThrow(/Object is not a Rust box/);
    expect(() => describeModel(null)).toThrow(/Object is not a Rust box/);
  });
});
