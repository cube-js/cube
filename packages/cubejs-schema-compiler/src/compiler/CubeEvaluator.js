/* eslint-disable no-restricted-syntax */
import R from 'ramda';

import { CubeSymbols } from './CubeSymbols';
import { UserError } from './UserError';
import { BaseQuery } from '../adapter';

export class CubeEvaluator extends CubeSymbols {
  constructor(cubeValidator) {
    super(true);
    this.cubeValidator = cubeValidator;
    /** @type {*} */
    this.evaluatedCubes = {};
    /** @type {*} */
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
    this.prepareJoins(cube, errorReporter);
    this.preparePreAggregations(cube, errorReporter);
    this.prepareMembers(cube.measures, cube, errorReporter);
    this.prepareMembers(cube.dimensions, cube, errorReporter);
    this.prepareMembers(cube.segments, cube, errorReporter);
  }

  /**
   * @protected
   */
  prepareJoins(cube, _errorReporter) {
    if (cube.joins) {
      // eslint-disable-next-line no-restricted-syntax
      for (const join of Object.values(cube.joins)) {
        // eslint-disable-next-line default-case
        switch (join.relationship) {
          case 'belongs_to':
          case 'many_to_one':
          case 'manyToOne':
            join.relationship = 'belongsTo';
            break;
          case 'has_many':
          case 'one_to_many':
          case 'oneToMany':
            join.relationship = 'hasMany';
            break;
          case 'has_one':
          case 'one_to_one':
          case 'oneToOne':
            join.relationship = 'hasOne';
            break;
        }
      }
    }
  }

  /**
   * @protected
   */
  preparePreAggregations(cube, errorReporter) {
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

  /**
   * @protected
   */
  prepareMembers(members, cube, errorReporter) {
    members = members || {};

    for (const memberName of Object.keys(members)) {
      let ownedByCube = true;
      let aliasMember;

      const member = members[memberName];
      if (member.sql && !member.subQuery) {
        const funcArgs = this.funcArguments(member.sql);
        const { cubeReferencesUsed, evaluatedSql, pathReferencesUsed } = this.collectUsedCubeReferences(cube.name, member.sql);
        // We won't check for FILTER_PARAMS here as it shouldn't affect ownership and it should obey the same reference rules.
        // To affect ownership FILTER_PARAMS can be declared as `${FILTER_PARAMS.Foo.bar.filter(`${Foo.bar}`)}`.
        // It isn't owned if there are non {CUBE} references
        if (funcArgs.length > 0 && cubeReferencesUsed.length === 0) {
          ownedByCube = false;
        }
        // Aliases one to one some another member as in case of views
        if (!ownedByCube && !member.filters && BaseQuery.isCalculatedMeasureType(member.type) && pathReferencesUsed.length === 1 && this.pathFromArray(pathReferencesUsed[0]) === evaluatedSql) {
          aliasMember = this.pathFromArray(pathReferencesUsed[0]);
        }
        const foreignCubes = cubeReferencesUsed.filter(usedCube => usedCube !== cube.name);
        if (foreignCubes.length > 0) {
          errorReporter.error(`Member '${cube.name}.${memberName}' references foreign cubes: ${foreignCubes.join(', ')}. Please split and move this definition to corresponding cubes.`);
        }
      }

      if (ownedByCube && cube.isView) {
        errorReporter.error(`View '${cube.name}' defines own member '${cube.name}.${memberName}'. Please move this member definition to one of the cubes.`);
      }

      members[memberName] = { ...members[memberName], ownedByCube };
      if (aliasMember) {
        members[memberName].aliasMember = aliasMember;
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

  /**
   * Returns pre-aggregations filtered by the spcified selector.
   * @param {{
   *  scheduled: boolean,
   *  dataSource: Array<string>,
   *  cubes: Array<string>,
   *  preAggregationIds: Array<string>
   * }} filter pre-aggregations selector
   * @returns {*}
   */
  preAggregations(filter) {
    const { scheduled, dataSources, cubes, preAggregationIds } = filter || {};
    const idFactory = ({ cube, preAggregationName }) => `${cube}.${preAggregationName}`;

    return Object.keys(this.evaluatedCubes)
      .filter((cube) => (
        (
          !cubes ||
          (cubes && cubes.length === 0) ||
          cubes.includes(cube)
        ) && (
          !dataSources ||
          (dataSources && dataSources.length === 0) ||
          dataSources.includes(
            this.evaluatedCubes[cube].dataSource || 'default'
          )
        )
      ))
      .map(cube => {
        const preAggregations = this.preAggregationsForCube(cube);
        return Object.keys(preAggregations)
          .filter(
            preAggregationName => (
              !scheduled ||
              preAggregations[preAggregationName].scheduledRefresh
            ) && (
              !preAggregationIds ||
              (preAggregationIds && preAggregationIds.length === 0) ||
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

  /**
   * @param {'measures'|'dimensions'|'segments'} type
   * @param {string} path
   * @returns boolean
   */
  isInstanceOfType(type, path) {
    const cubeAndName = Array.isArray(path) ? path : path.split('.');
    return this.evaluatedCubes[cubeAndName[0]] &&
      this.evaluatedCubes[cubeAndName[0]][type] &&
      this.evaluatedCubes[cubeAndName[0]][type][cubeAndName[1]];
  }

  /**
   * @param {string} path
   * @returns {*}
   */
  byPathAnyType(path) {
    const type = ['measures', 'dimensions', 'segments'].find(t => this.isInstanceOfType(t, path));

    if (!type) {
      throw new UserError(`Can't resolve member '${path.join('.')}'`);
    }

    return this.byPath(type, path);
  }

  /**
   * @param {'measures'|'dimensions'|'segments'} type
   * @param {string} path
   * @returns {*}
   */
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

  parsePathAnyType(path) {
    // Should throw UserError in case of parse error
    this.byPathAnyType(path);
    return path.split('.');
  }

  collectUsedCubeReferences(cube, sqlFn) {
    const cubeEvaluator = this;

    const cubeReferencesUsed = [];
    const pathReferencesUsed = [];

    const evaluatedSql = cubeEvaluator.resolveSymbolsCall(sqlFn, (name) => {
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
      const path = [referencedCube, name];
      pathReferencesUsed.push(path);
      return cubeEvaluator.pathFromArray(path);
    }, {
      // eslint-disable-next-line no-shadow
      sqlResolveFn: (symbol, cube, n) => {
        const path = [cube, n];
        pathReferencesUsed.push(path);
        return cubeEvaluator.pathFromArray(path);
      },
      contextSymbols: BaseQuery.emptyParametrizedContextSymbols(this, () => '$empty_param$'),
      cubeReferencesUsed,
    });
    return { cubeReferencesUsed, pathReferencesUsed, evaluatedSql };
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
        aggregation.rollupReferences && this.evaluateReferences(cube, aggregation.rollupReferences, { originalSorting: true }) || [],
    };
  }
}
