/* eslint-disable no-restricted-syntax */
import R from 'ramda';

import { CubeSymbols } from './CubeSymbols';
import { UserError } from './UserError';
import { BaseQuery } from '../adapter';

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
    this.prepareJoins(cube, errorReporter);
    this.preparePreAggregations(cube, errorReporter);
    this.prepareMembers(cube.measures, cube, errorReporter);
    this.prepareMembers(cube.dimensions, cube, errorReporter);
    this.prepareMembers(cube.segments, cube, errorReporter);
    this.prepareIncludes(cube, errorReporter);
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

      const member = members[memberName];
      if (member.sql && !member.subQuery) {
        const funcArgs = this.funcArguments(member.sql);
        const cubeReferences = this.collectUsedCubeReferences(cube.name, member.sql);
        // We won't check for FILTER_PARAMS here as it shouldn't affect ownership and it should obey the same reference rules.
        // To affect ownership FILTER_PARAMS can be declared as `${FILTER_PARAMS.Foo.bar.filter(`${Foo.bar}`)}`.
        if (funcArgs.length > 0 && cubeReferences.length === 0) {
          ownedByCube = false;
        }
        const foreignCubes = cubeReferences.filter(usedCube => usedCube !== cube.name);
        if (foreignCubes.length > 0) {
          errorReporter.error(`Member '${cube.name}.${memberName}' references foreign cubes: ${foreignCubes.join(', ')}. Please split and move this definition to corresponding cubes.`);
        }
      }

      if (ownedByCube && cube.isView) {
        errorReporter.error(`View '${cube.name}' defines own member '${cube.name}.${memberName}'. Please move this member definition to one of the cubes.`);
      }

      members[memberName] = { ...members[memberName], ownedByCube };
    }
  }

  /**
   * @protected
   */
  prepareIncludes(cube, errorReporter) {
    if (!cube.includes && !cube.cubes) {
      return;
    }
    const types = ['measures', 'dimensions', 'segments'];
    for (const type of types) {
      const cubeIncludes = cube.cubes && this.membersFromCubes(cube.cubes, type, errorReporter) || [];
      const includes = cube.includes && this.membersFromIncludeExclude(cube.includes, cube.name, type) || [];
      const excludes = cube.excludes && this.membersFromIncludeExclude(cube.excludes, cube.name, type) || [];
      // cube includes will take precedence in case of member clash
      const finalIncludes = this.diffByMember(this.diffByMember(includes, cubeIncludes).concat(cubeIncludes), excludes);
      const includeMembers = this.generateIncludeMembers(finalIncludes, cube.name, type);
      for (const [memberName, memberDefinition] of includeMembers) {
        if (cube[type]?.[memberName]) {
          errorReporter.error(`Included member '${memberName}' conflicts with existing member of '${cube.name}'. Please consider excluding this member.`);
        } else {
          cube[type][memberName] = memberDefinition;
        }
      }
    }
  }

  /**
   * @protected
   */
  membersFromCubes(cubes, type, errorReporter) {
    return R.unnest(cubes.map(cubeInclude => {
      const fullPath = this.evaluateReferences(null, cubeInclude.joinPath, { collectJoinHints: true });
      const split = fullPath.split('.');
      const cubeReference = split[split.length - 1];
      const cubeName = cubeInclude.alias || cubeReference;
      let includes;
      const fullMemberName = (memberName) => (cubeInclude.prefix ? `${cubeName}_${memberName}` : memberName);
      if (cubeInclude.includes === '*') {
        const membersObj = this.symbols[cubeReference]?.cubeObj()?.[type] || {};
        includes = Object.keys(membersObj).map(memberName => ({ member: `${fullPath}.${memberName}`, name: fullMemberName(memberName) }));
      } else {
        includes = cubeInclude.includes.map(include => {
          const member = include.alias || include;
          if (member.indexOf('.') !== -1) {
            errorReporter.error(`Paths aren't allowed in cube includes but '${member}' provided as include member`);
          }
          const name = fullMemberName(include.alias || member);
          if (include.name) {
            const resolvedMember = this.symbols[cubeReference]?.cubeObj()?.[type]?.[include.name];
            return resolvedMember ? {
              member: `${fullPath}.${include.name}`,
              name,
            } : undefined;
          } else {
            const resolvedMember = this.symbols[cubeReference]?.cubeObj()?.[type]?.[include];
            return resolvedMember ? {
              member: `${fullPath}.${include}`,
              name
            } : undefined;
          }
        });
      }

      const excludes = (cubeInclude.excludes || []).map(exclude => {
        if (exclude.indexOf('.') !== -1) {
          errorReporter.error(`Paths aren't allowed in cube excludes but '${exclude}' provided as exclude member`);
        }
        const resolvedMember = this.symbols[cubeReference]?.cubeObj()?.[type]?.[exclude];
        return resolvedMember ? {
          member: `${cubeReference}.${exclude}`
        } : undefined;
      });
      return this.diffByMember(includes.filter(Boolean), excludes.filter(Boolean));
    }));
  }

  diffByMember(includes, excludes) {
    const excludesMap = new Map();
    for (const exclude of excludes) {
      excludesMap.set(exclude.member, true);
    }
    return includes.filter(include => !excludesMap.get(include.member));
  }

  membersFromIncludeExclude(referencesFn, cubeName, type) {
    const references = this.evaluateReferences(cubeName, referencesFn);
    return R.unnest(references.map(ref => {
      const path = ref.split('.');
      if (path.length === 1) {
        const membersObj = this.symbols[path[0]]?.cubeObj()?.[type] || {};
        return Object.keys(membersObj).map(memberName => ({ member: `${ref}.${memberName}` }));
      } else if (path.length === 2) {
        const resolvedMember = this.symbols[path[0]]?.cubeObj()?.[type]?.[path[1]];
        return resolvedMember ? [{ member: ref }] : undefined;
      } else {
        throw new Error(`Unexpected path length ${path.length} for ${ref}`);
      }
    })).filter(Boolean);
  }

  /**
   * @protected
   */
  generateIncludeMembers(members, cubeName, type) {
    return members.map(memberRef => {
      const path = memberRef.member.split('.');
      const resolvedMember = this.symbols[path[path.length - 2]]?.cubeObj()?.[type]?.[path[path.length - 1]];
      if (!resolvedMember) {
        throw new Error(`Can't resolve '${memberRef.member}' while generating include members`);
      }

      // eslint-disable-next-line no-new-func
      const sql = new Function(path[0], `return \`\${${memberRef.member}}\`;`);
      let memberDefinition;
      if (type === 'measures') {
        memberDefinition = {
          sql,
          type: 'number',
          aggType: resolvedMember.type,
          meta: resolvedMember.meta,
          description: resolvedMember.description,
        };
      } else if (type === 'dimensions') {
        memberDefinition = {
          sql,
          type: resolvedMember.type,
          meta: resolvedMember.meta,
          description: resolvedMember.description,
        };
      } else if (type === 'segments') {
        memberDefinition = {
          sql,
          meta: resolvedMember.meta,
          description: resolvedMember.description,
        };
      } else {
        throw new Error(`Unexpected member type: ${type}`);
      }
      return [memberRef.name || path[path.length - 1], memberDefinition];
    });
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

  pathFromArray(array) {
    return array.join('.');
  }

  isInstanceOfType(type, path) {
    const cubeAndName = Array.isArray(path) ? path : path.split('.');
    return this.evaluatedCubes[cubeAndName[0]] &&
      this.evaluatedCubes[cubeAndName[0]][type] &&
      this.evaluatedCubes[cubeAndName[0]][type][cubeAndName[1]];
  }

  byPathAnyType(path) {
    const type = ['measures', 'dimensions', 'segments'].find(t => this.isInstanceOfType(t, path));

    if (!type) {
      throw new UserError(`Can't resolve member '${path.join('.')}'`);
    }

    return this.byPath(type, path);
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

  parsePathAnyType(path) {
    // Should throw UserError in case of parse error
    this.byPathAnyType(path);
    return path.split('.');
  }

  collectUsedCubeReferences(cube, sqlFn) {
    const cubeEvaluator = this;

    const cubeReferencesUsed = [];

    cubeEvaluator.resolveSymbolsCall(sqlFn, (name) => {
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
      sqlResolveFn: (symbol, cube, n) => cubeEvaluator.pathFromArray([cube, n]),
      contextSymbols: BaseQuery.emptyParametrizedContextSymbols(this, () => '$empty_param$'),
      cubeReferencesUsed,
    });
    return cubeReferencesUsed;
  }

  evaluateReferences(cube, referencesFn, options = {}) {
    const cubeEvaluator = this;

    const fullPath = (joinHints, path) => {
      if (joinHints?.length > 0) {
        return R.uniq(joinHints.concat(path));
      } else {
        return path;
      }
    };

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
      return cubeEvaluator.pathFromArray(fullPath(cubeEvaluator.joinHints(), [referencedCube, name]));
    }, {
      // eslint-disable-next-line no-shadow
      sqlResolveFn: (symbol, cube, n) => cubeEvaluator.pathFromArray(fullPath(cubeEvaluator.joinHints(), [cube, n])),
      // eslint-disable-next-line no-shadow
      cubeAliasFn: (cube) => cubeEvaluator.pathFromArray(fullPath(cubeEvaluator.joinHints(), [cube])),
      collectJoinHints: options.collectJoinHints,
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
        aggregation.rollupReferences && this.evaluateReferences(cube, aggregation.rollupReferences, { originalSorting: true }) || [],
    };
  }
}
