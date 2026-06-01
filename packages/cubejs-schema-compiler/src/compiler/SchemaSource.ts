import { CubeEvaluator } from './CubeEvaluator';

/**
 * Build-phase wrapper exposed to the Rust side as the data source for
 * the Tesseract domain model. Kept intentionally separate from
 * `CubeEvaluator`: that one stays the runtime/lookup bridge, this one
 * is consumed once per schema by `ModelBuilder` on the Rust side.
 *
 * Each returned cube is a thin prototype wrapper: own properties
 * override `measures` / `dimensions` / `segments` / `preAggregations`
 * (Record → Array form required by the Rust bridge), while everything
 * else (sql, getters like `maskSql`, ...) is inherited from the
 * underlying evaluated cube via the prototype chain so getters keep
 * working. Dimensions and pre-aggregations get the same prototype
 * trick to surface nested Records (granularities, indexes) as arrays
 * with `name` stamped on each entry.
 */
export class SchemaSource {
  public constructor(private readonly cubeEvaluator: CubeEvaluator) {}

  public get primaryKeys(): Record<string, string[]> {
    return this.cubeEvaluator.primaryKeys;
  }

  public cubes(): any[] {
    return Object.values(this.cubeEvaluator.evaluatedCubes).map(cube => {
      const wrapper = Object.create(cube);
      wrapper.measures = Object.values(cube.measures || {});
      wrapper.dimensions = Object.values(cube.dimensions || {}).map(SchemaSource.wrapDimension);
      wrapper.segments = Object.values(cube.segments || {});
      wrapper.preAggregations = Object.values(cube.preAggregations || {}).map(SchemaSource.wrapPreAggregation);
      return wrapper;
    });
  }

  private static wrapDimension(dim: any): any {
    if (!dim.granularities) {
      return dim;
    }
    const wrapped = Object.create(dim);
    wrapped.granularities = Object.entries(dim.granularities).map(([name, gran]: [string, any]) => {
      if (gran.name === undefined) {
        gran.name = name;
      }
      return gran;
    });
    return wrapped;
  }

  private static wrapPreAggregation(preAgg: any): any {
    if (!preAgg.indexes) {
      return preAgg;
    }
    const wrapped = Object.create(preAgg);
    wrapped.indexes = Object.entries(preAgg.indexes).map(([name, idx]: [string, any]) => {
      if (idx.name === undefined) {
        idx.name = name;
      }
      return idx;
    });
    return wrapped;
  }
}
