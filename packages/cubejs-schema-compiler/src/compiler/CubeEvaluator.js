/* eslint-disable no-restricted-syntax */
import R from 'ramda';

import { CubeSymbols } from './CubeSymbols';
import { UserError } from './UserError';

export class CubeEvaluator extends CubeSymbols {
  constructor(cubeValidator) {
    super(cubeValidator);
    this.cubeValidator = cubeValidator;
    this.evaluatedCubes = {};
    this.primaryKeys = {};
  }

  compile(cubes, errorReporter) {
    super.compile(cubes, errorReporter);
    const validCubes = this.cubeList.filter(cube => this.cubeValidator.isCubeValid(cube));

    Object.values(validCubes).map((cube) => this.prepareCube(cube, errorReporter));

    this.evaluatedCubes = R.fromPairs(validCubes.map(v => [v.name, v]));
    this.byFileName = R.groupBy(v => v.fileName, validCubes);
    this.primaryKeys = R.fromPairs(
      validCubes.map((v) => {
        const primaryKeyNamesToSymbols = R.compose(
          R.map((d) => d[0]),
          R.filter((d) => d[1].primaryKey),
          R.toPairs
        )(v.dimensions || {});
        return [v.name, primaryKeyNamesToSymbols];
      })
    );
  }

  /**
   * @protected
   */
  prepareCube(cube, errorReporter) {
    if (cube.preAggregations) {
      // eslint-disable-next-line no-restricted-syntax
      for (const preAggregation of Object.values(cube.preAggregations)) {
        if (preAggregation.timeDimension) {
          preAggregation.timeDimensionReference = preAggregation.timeDimension;
          delete preAggregation.timeDimension;
        }

        if (preAggregation.dimensions) {
          preAggregation.dimensionReferences = preAggregation.dimensions;
          delete preAggregation.dimensions;
        }

        if (preAggregation.measures) {
          preAggregation.measureReferences = preAggregation.measures;
          delete preAggregation.measures;
        }

        if (preAggregation.segments) {
          preAggregation.segmentReferences = preAggregation.segments;
          delete preAggregation.segments;
        }

        if (preAggregation.rollups) {
          preAggregation.rollupReferences = preAggregation.rollups;
          delete preAggregation.rollups;
        }

        if (preAggregation.buildRangeStart) {
          if (preAggregation.refreshRangeStart) {
            errorReporter.warning({
              message: 'You specified both buildRangeStart and refreshRangeStart, buildRangeStart will be used.'
            });
          }

          preAggregation.refreshRangeStart = preAggregation.buildRangeStart;
          delete preAggregation.buildRangeStart;
        }

        if (preAggregation.buildRangeEnd) {
          if (preAggregation.refreshRangeEnd) {
            errorReporter.warning({
              message: 'You specified both buildRangeEnd and refreshRangeEnd, buildRangeEnd will be used.'
            });
          }

          preAggregation.refreshRangeEnd = preAggregation.buildRangeEnd;
          delete preAggregation.buildRangeEnd;
        }
      }
    }
  }

  cubesByFileName(fileName) {
    return this.byFileName[fileName] || [];
  }

  timeDimensionPathsForCube(cube) {
    return R.compose(
      R.map(nameToDefinition => `${cube}.${nameToDefinition[0]}`),
      R.toPairs,
      R.filter(d => d.type === 'time')
    )(this.evaluatedCubes[cube].dimensions || {});
  }

  measuresForCube(cube) {
    return this.cubeFromPath(cube).measures || {};
  }

  preAggregationsForCube(path) {
    return this.cubeFromPath(path).preAggregations || {};
  }

  preAggregations(filter) {
    const { scheduled, cubes, preAggregationIds } = filter || {};
    const idFactory = ({ cube, preAggregationName }) => `${cube}.${preAggregationName}`;

    return Object.keys(this.evaluatedCubes)
      .filter(cube => !cubes || cubes.includes(cube))
      .map(cube => {
        const preAggregations = this.preAggregationsForCube(cube);
        return Object.keys(preAggregations)
          .filter(
            preAggregationName => (
              !scheduled ||
              preAggregations[preAggregationName].scheduledRefresh
            ) && (
              !preAggregationIds ||
              preAggregationIds.includes(idFactory({
                cube, preAggregationName
              }))
            )
          )
          .map(preAggregationName => {
            const { indexes, refreshKey } = preAggregations[preAggregationName];
            return {
              id: idFactory({ cube, preAggregationName }),
              preAggregationName,
              preAggregation: preAggregations[preAggregationName],
              cube,
              references: this.evaluatePreAggregationReferences(cube, preAggregations[preAggregationName]),
              refreshKey,
              indexesReferences: indexes && Object.keys(indexes).reduce((obj, indexName) => {
                obj[indexName] = {
                  columns: this.evaluateReferences(
                    cube,
                    indexes[indexName].columns,
                    { originalSorting: true }
                  ),
                  type: indexes[indexName].type
                };
                return obj;
              }, {})
            };
          });
      })
      .reduce((a, b) => a.concat(b), []);
  }

  scheduledPreAggregations() {
    return this.preAggregations({ scheduled: true });
  }

  cubeNames() {
    return Object.keys(this.evaluatedCubes);
  }

  isMeasure(measurePath) {
    return this.isInstanceOfType('measures', measurePath);
  }

  isDimension(path) {
    return this.isInstanceOfType('dimensions', path);
  }

  isSegment(path) {
    return this.isInstanceOfType('segments', path);
  }

  measureByPath(measurePath) {
    return this.byPath('measures', measurePath);
  }

  dimensionByPath(dimensionPath) {
    return this.byPath('dimensions', dimensionPath);
  }

  segmentByPath(segmentPath) {
    return this.byPath('segments', segmentPath);
  }

  cubeExists(cube) {
    return !!this.evaluatedCubes[cube];
  }

  cubeFromPath(path) {
    return this.evaluatedCubes[this.cubeNameFromPath(path)];
  }

  cubeNameFromPath(path) {
    const cubeAndName = path.split('.');
    if (!this.evaluatedCubes[cubeAndName[0]]) {
      throw new UserError(`Cube '${cubeAndName[0]}' not found for path '${path}'`);
    }
    return cubeAndName[0];
  }

  pathFromArray(array) {
    return array.join('.');
  }

  isInstanceOfType(type, path) {
    const cubeAndName = Array.isArray(path) ? path : path.split('.');
    return this.evaluatedCubes[cubeAndName[0]] &&
      this.evaluatedCubes[cubeAndName[0]][type] &&
      this.evaluatedCubes[cubeAndName[0]][type][cubeAndName[1]];
  }

  byPath(type, path) {
    if (!type) {
      throw new Error(`Type can't be undefined for '${path}'`);
    }

    if (!path) {
      throw new Error('Path can\'t be undefined');
    }

    const cubeAndName = Array.isArray(path) ? path : path.split('.');
    if (!this.evaluatedCubes[cubeAndName[0]]) {
      throw new UserError(`Cube '${cubeAndName[0]}' not found for path '${path}'`);
    }

    if (!this.evaluatedCubes[cubeAndName[0]][type]) {
      throw new UserError(`${type} not defined for path '${path}'`);
    }

    if (!this.evaluatedCubes[cubeAndName[0]][type][cubeAndName[1]]) {
      throw new UserError(`'${cubeAndName[1]}' not found for path '${path}'`);
    }

    return this.evaluatedCubes[cubeAndName[0]][type][cubeAndName[1]];
  }

  parsePath(type, path) {
    // Should throw UserError in case of parse error
    this.byPath(type, path);
    return path.split('.');
  }

  evaluateReferences(cube, referencesFn, options = {}) {
    const cubeEvaluator = this;

    const arrayOrSingle = cubeEvaluator.resolveSymbolsCall(referencesFn, (name) => {
      const referencedCube = cubeEvaluator.symbols[name] && name || cube;
      const resolvedSymbol =
        cubeEvaluator.resolveSymbol(
          cube,
          name
        );
      // eslint-disable-next-line no-underscore-dangle
      if (resolvedSymbol._objectWithResolvedProperties) {
        return resolvedSymbol;
      }
      return cubeEvaluator.pathFromArray([referencedCube, name]);
    }, {
      // eslint-disable-next-line no-shadow
      sqlResolveFn: (symbol, cube, n) => cubeEvaluator.pathFromArray([cube, n])
    });
    if (!Array.isArray(arrayOrSingle)) {
      return arrayOrSingle.toString();
    }

    const references = arrayOrSingle.map(p => p.toString());
    return options.originalSorting ? references : R.sortBy(R.identity, references);
  }

  evaluatePreAggregationReferences(cube, aggregation) {
    const timeDimensions = aggregation.timeDimensionReference ? [{
      dimension: this.evaluateReferences(cube, aggregation.timeDimensionReference),
      granularity: aggregation.granularity
    }] : [];
    return {
      allowNonStrictDateRangeMatch: aggregation.allowNonStrictDateRangeMatch,
      dimensions:
        (aggregation.dimensionReferences && this.evaluateReferences(cube, aggregation.dimensionReferences) || [])
          .concat(
            aggregation.segmentReferences && this.evaluateReferences(cube, aggregation.segmentReferences) || []
          ),
      measures:
        aggregation.measureReferences && this.evaluateReferences(cube, aggregation.measureReferences) || [],
      timeDimensions,
      rollups:
        aggregation.rollupReferences && this.evaluateReferences(cube, aggregation.rollupReferences) || [],
    };
  }
}
