const R = require('ramda');

const CubeSymbols = require('./CubeSymbols');
const UserError = require('./UserError');

class CubeEvaluator extends CubeSymbols {
  constructor(cubeValidator) {
    super(cubeValidator);
    this.cubeValidator = cubeValidator;
    this.evaluatedCubes = {};
    this.primaryKeys = {};
  }

  compile(cubes, errorReporter) {
    super.compile(cubes, errorReporter);
    const validCubes = this.cubeList.filter(cube => this.cubeValidator.isCubeValid(cube));

    this.evaluatedCubes = R.fromPairs(validCubes.map(v => [v.name, v]));
    this.byFileName = R.groupBy(v => v.fileName, validCubes);
    this.primaryKeys = R.fromPairs(validCubes.map((v) => {
      const primaryKeyNameToSymbol = R.compose(R.find(d => d[1].primaryKey), R.toPairs)(v.dimensions || {});
      return [
        v.name,
        primaryKeyNameToSymbol && primaryKeyNameToSymbol[0]
      ];
    }));
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

  scheduledPreAggregations() {
    return Object.keys(this.evaluatedCubes).map(cube => {
      const preAggregations = this.preAggregationsForCube(cube);
      return Object.keys(preAggregations)
        .filter(name => preAggregations[name].scheduledRefresh)
        .map(preAggregationName => ({
          preAggregationName,
          preAggregation: preAggregations[preAggregationName],
          cube,
          references: this.evaluatePreAggregationReferences(cube, preAggregations[preAggregationName])
        }));
    }).reduce((a, b) => a.concat(b), []);
  }

  cubeNamesWithRefreshKeys() {
    return Object.keys(this.evaluatedCubes).filter(c => !!this.evaluatedCubes[c].refreshKey);
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
      throw new Error(`Path can't be undefined`);
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
      dimensions:
        (aggregation.dimensionReferences && this.evaluateReferences(cube, aggregation.dimensionReferences) || [])
          .concat(
            aggregation.segmentReferences && this.evaluateReferences(cube, aggregation.segmentReferences) || []
          ),
      measures:
      aggregation.measureReferences && this.evaluateReferences(cube, aggregation.measureReferences) || [],
      timeDimensions
    };
  }
}

module.exports = CubeEvaluator;
