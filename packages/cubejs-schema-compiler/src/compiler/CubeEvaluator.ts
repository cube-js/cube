/* eslint-disable no-restricted-syntax */
import R from 'ramda';

import { CubeSymbols } from './CubeSymbols';
import { UserError } from './UserError';
import { BaseQuery } from '../adapter';
import type { CubeValidator } from './CubeValidator';
import type { ErrorReporter } from './ErrorReporter';

export type SegmentDefinition = {
  type: string,
  sql: Function,
  primaryKey?: true,
  ownedByCube: boolean,
  fieldType?: string,
  // TODO should we have it here?
  multiStage?: boolean,
};

export type DimensionDefinition = {
  type: string,
  sql: Function,
  primaryKey?: true,
  ownedByCube: boolean,
  fieldType?: string,
  multiStage?: boolean,
  shiftInterval?: string,
};

export type TimeShiftDefinition = {
  timeDimension: Function,
  interval: string,
  type: 'next' | 'prior',
};

export type TimeShiftDefinitionReference = {
  timeDimension: string,
  interval: string,
  type: 'next' | 'prior',
};

export type MeasureDefinition = {
  type: string,
  sql: Function,
  ownedByCube: boolean,
  rollingWindow?: any
  filters?: any
  primaryKey?: true,
  drillFilters?: any,
  multiStage?: boolean,
  groupBy?: Function,
  reduceBy?: Function,
  addGroupBy?: Function,
  timeShift?: TimeShiftDefinition[],
  groupByReferences?: string[],
  reduceByReferences?: string[],
  addGroupByReferences?: string[],
  timeShiftReferences?: TimeShiftDefinitionReference[],
};

export class CubeEvaluator extends CubeSymbols {
  public evaluatedCubes: Record<string, any> = {};

  public primaryKeys: Record<string, any> = {};

  public byFileName: Record<string, any> = {};

  private isRbacEnabledCache: boolean | null = null;

  public constructor(
    protected readonly cubeValidator: CubeValidator
  ) {
    super(true);
  }

  public compile(cubes: any[], errorReporter: ErrorReporter) {
    super.compile(cubes, errorReporter);
    const validCubes = this.cubeList.filter(cube => this.cubeValidator.isCubeValid(cube)).sort((a, b) => {
      if (a.isView) {
        return 1;
      } else if (!a.isView && !b.isView) {
        return 0;
      } else {
        return -1;
      }
    });

    for (const cube of validCubes) {
      this.evaluatedCubes[cube.name] = this.prepareCube(cube, errorReporter);
    }

    this.byFileName = R.groupBy(v => v.fileName, validCubes);
    this.primaryKeys = R.fromPairs(
      validCubes.map((v) => {
        const primaryKeyNamesToSymbols = R.compose(
          R.map((d: any) => d[0]),
          R.filter((d: any) => d[1].primaryKey),
          R.toPairs
        )(v.dimensions || {});
        return [v.name, primaryKeyNamesToSymbols];
      })
    );
  }

  protected prepareCube(cube, errorReporter: ErrorReporter) {
    this.prepareJoins(cube, errorReporter);
    this.preparePreAggregations(cube, errorReporter);
    this.prepareMembers(cube.measures, cube, errorReporter);
    this.prepareMembers(cube.dimensions, cube, errorReporter);
    this.prepareMembers(cube.segments, cube, errorReporter);

    this.evaluateMultiStageReferences(cube.name, cube.measures);
    this.evaluateMultiStageReferences(cube.name, cube.dimensions);

    this.prepareHierarchies(cube, errorReporter);
    this.prepareFolders(cube, errorReporter);

    this.prepareAccessPolicy(cube, errorReporter);

    return cube;
  }

  private allMembersOrList(cube: any, specifier: string | string[]): string[] {
    const types = ['measures', 'dimensions', 'segments'];
    if (specifier === '*') {
      const allMembers = R.unnest(types.map(type => Object.keys(cube[type] || {})));
      return allMembers;
    } else {
      return specifier as string[] || [];
    }
  }

  private prepareAccessPolicy(cube: any, errorReporter: ErrorReporter) {
    if (!cube.accessPolicy) {
      return;
    }

    const memberMapper = (memberType: string) => (member: string) => {
      if (member.indexOf('.') !== -1) {
        const cubeName = member.split('.')[0];
        if (cubeName !== cube.name) {
          errorReporter.error(
            `Paths aren't allowed in the accessPolicy policy but '${member}' provided as ${memberType} for ${cube.name}`
          );
        }
        return member;
      }
      return this.pathFromArray([cube.name, member]);
    };

    const filterEvaluator = (filter: any) => {
      if (filter.member) {
        filter.memberReference = this.evaluateReferences(cube.name, filter.member);
        filter.memberReference = memberMapper('a filter member reference')(filter.memberReference);
      } else {
        if (filter.and) {
          filter.and.forEach(filterEvaluator);
        }
        if (filter.or) {
          filter.or.forEach(filterEvaluator);
        }
      }
    };

    for (const policy of cube.accessPolicy) {
      for (const filter of policy?.rowLevel?.filters || []) {
        filterEvaluator(filter);
      }

      if (policy.memberLevel) {
        policy.memberLevel.includesMembers = this.allMembersOrList(
          cube,
          policy.memberLevel.includes || '*'
        ).map(memberMapper('an includes member'));
        policy.memberLevel.excludesMembers = this.allMembersOrList(
          cube,
          policy.memberLevel.excludes || []
        ).map(memberMapper('an excludes member'));
      }
    }
  }

  private prepareFolders(cube: any, errorReporter: ErrorReporter) {
    if (Array.isArray(cube.folders)) {
      cube.folders = cube.folders.map(it => {
        const includedMembers = this.allMembersOrList(cube, it.includes);
        const includes = includedMembers.map(memberName => {
          if (memberName.includes('.')) {
            errorReporter.error(
              `Paths aren't allowed in the 'folders' but '${memberName}' has been provided for ${cube.name}`
            );
          }

          const member = cube.includedMembers.find(m => m.name === memberName);
          if (!member) {
            errorReporter.error(
              `Member '${memberName}' included in folder '${it.name}' not found`
            );
            return null;
          }

          return member;
        })
          .filter(Boolean);

        return ({
          ...it,
          includes
        });
      });
    }

    return [];
  }

  private prepareHierarchies(cube: any, errorReporter: ErrorReporter): void {
    const uniqueHierarchyNames = new Set();
    if (Object.keys(cube.hierarchies).length) {
      cube.evaluatedHierarchies = Object.entries(cube.hierarchies).map(([name, hierarchy]) => {
        if (uniqueHierarchyNames.has(name)) {
          errorReporter.error(`Duplicate hierarchy name '${name}' in cube '${cube.name}'`);
        }
        uniqueHierarchyNames.add(name);

        return ({
          name,
          ...(typeof hierarchy === 'object' ? hierarchy : {}),
          levels: this.evaluateReferences(
            cube.name,
            // @ts-ignore
            hierarchy.levels,
            { originalSorting: true }
          )
        });
      });
    }

    if (cube.isView && (cube.includedMembers || []).length) {
      const includedMemberPaths: string[] = R.uniq(cube.includedMembers.map(it => it.memberPath));
      const includedCubeNames: string[] = R.uniq(includedMemberPaths.map(it => it.split('.')[0]));
      const includedHierarchyNames = cube.includedMembers.filter(it => it.type === 'hierarchies').map(it => it.memberPath.split('.')[1]);

      for (const cubeName of includedCubeNames) {
        // As views come after cubes in the list, we can safely assume that cube is already evaluated
        const { evaluatedHierarchies: hierarchies } = this.evaluatedCubes[cubeName] || {};

        if (Array.isArray(hierarchies) && hierarchies.length) {
          const filteredHierarchies = hierarchies
            .filter(it => includedHierarchyNames.includes(it.name))
            .map(it => {
              const levels = it.levels.filter(level => {
                const member = cube.includedMembers.find(m => m.memberPath === level);
                if (member && member.type !== 'dimensions') {
                  const memberName = level.split('.')[1] || level;
                  errorReporter.error(`Only dimensions can be part of a hierarchy. Please remove the '${memberName}' member from the '${it.name}' hierarchy.`);
                } else if (member) {
                  return includedMemberPaths.includes(level);
                }

                return null;
              }).filter(Boolean);

              return {
                ...it,
                levels
              };
            })
            .filter(it => it.levels.length);

          cube.evaluatedHierarchies = [...(cube.evaluatedHierarchies || []), ...filteredHierarchies];
        }
      }

      cube.evaluatedHierarchies = (cube.evaluatedHierarchies || []).map((hierarchy) => ({
        ...hierarchy,
        levels: hierarchy.levels.map((level) => {
          const member = cube.includedMembers.find(m => m.memberPath === level);

          if (!member) {
            return null;
          }

          return [cube.name, member.name].join('.');
        }).filter(Boolean)
      }));
    }
  }

  private evaluateMultiStageReferences(cubeName: string, obj: { [key: string]: MeasureDefinition }) {
    if (!obj) {
      return;
    }

    // eslint-disable-next-line no-restricted-syntax
    for (const member of Object.values(obj)) {
      if (member.multiStage) {
        if (member.groupBy) {
          member.groupByReferences = this.evaluateReferences(cubeName, member.groupBy);
        }
        if (member.reduceBy) {
          member.reduceByReferences = this.evaluateReferences(cubeName, member.reduceBy);
        }
        if (member.addGroupBy) {
          member.addGroupByReferences = this.evaluateReferences(cubeName, member.addGroupBy);
        }
        if (member.timeShift) {
          member.timeShiftReferences = member.timeShift
            .map(s => ({ ...s, timeDimension: this.evaluateReferences(cubeName, s.timeDimension) }));
        }
      }
    }
  }

  protected prepareJoins(cube: any, _errorReporter: ErrorReporter) {
    if (cube.joins) {
      // eslint-disable-next-line no-restricted-syntax
      for (const join of Object.values(cube.joins) as any[]) {
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

  protected preparePreAggregations(cube: any, errorReporter: ErrorReporter) {
    if (cube.preAggregations) {
      // eslint-disable-next-line no-restricted-syntax
      for (const preAggregation of Object.values(cube.preAggregations) as any) {
        if (preAggregation.timeDimension) {
          preAggregation.timeDimensionReference = preAggregation.timeDimension;
          delete preAggregation.timeDimension;
        }

        if (preAggregation.timeDimensions) {
          preAggregation.timeDimensionReferences = preAggregation.timeDimensions;
          delete preAggregation.timeDimensions;
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
              message: 'You specified both buildRangeStart and refreshRangeStart, buildRangeStart will be used.',
              loc: null,
            });
          }

          preAggregation.refreshRangeStart = preAggregation.buildRangeStart;
          delete preAggregation.buildRangeStart;
        }

        if (preAggregation.buildRangeEnd) {
          if (preAggregation.refreshRangeEnd) {
            errorReporter.warning({
              message: 'You specified both buildRangeEnd and refreshRangeEnd, buildRangeEnd will be used.',
              loc: null,
            });
          }

          preAggregation.refreshRangeEnd = preAggregation.buildRangeEnd;
          delete preAggregation.buildRangeEnd;
        }

        if (preAggregation.outputColumnTypes) {
          preAggregation.outputColumnTypes.forEach(column => {
            column.name = this.evaluateReferences(cube.name, column.member, { originalSorting: true });
          });
        }
      }
    }
  }

  protected prepareMembers(members: any, cube: any, errorReporter: ErrorReporter) {
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

  public cubesByFileName(fileName) {
    return this.byFileName[fileName] || [];
  }

  public timeDimensionPathsForCube(cube: any) {
    return R.compose(
      R.map(nameToDefinition => `${cube}.${nameToDefinition[0]}`),
      R.toPairs,
      // @ts-ignore
      R.filter((d: any) => d.type === 'time')
      // @ts-ignore
    )(this.evaluatedCubes[cube].dimensions || {});
  }

  public measuresForCube(cube) {
    return this.cubeFromPath(cube).measures || {};
  }

  public preAggregationsForCube(path: string) {
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
  public preAggregations(filter) {
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

  public scheduledPreAggregations() {
    return this.preAggregations({ scheduled: true });
  }

  public cubeNames() {
    return Object.keys(this.evaluatedCubes);
  }

  public isMeasure(measurePath: string | string[]): boolean {
    return this.isInstanceOfType('measures', measurePath);
  }

  public isDimension(path: string | string[]): boolean {
    return this.isInstanceOfType('dimensions', path);
  }

  public isSegment(path: string | string[]): boolean {
    return this.isInstanceOfType('segments', path);
  }

  public measureByPath(measurePath: string): MeasureDefinition {
    return this.byPath('measures', measurePath);
  }

  public dimensionByPath(dimensionPath: string): DimensionDefinition {
    return this.byPath('dimensions', dimensionPath);
  }

  public segmentByPath(segmentPath: string): SegmentDefinition {
    return this.byPath('segments', segmentPath);
  }

  public cubeExists(cube) {
    return !!this.evaluatedCubes[cube];
  }

  public cubeFromPath(path: string) {
    return this.evaluatedCubes[this.cubeNameFromPath(path)];
  }

  public cubeNameFromPath(path: string) {
    const cubeAndName = path.split('.');
    if (!this.evaluatedCubes[cubeAndName[0]]) {
      throw new UserError(`Cube '${cubeAndName[0]}' not found for path '${path}'`);
    }
    return cubeAndName[0];
  }

  public isInstanceOfType(type: 'measures' | 'dimensions' | 'segments', path: string | string[]): boolean {
    const cubeAndName = Array.isArray(path) ? path : path.split('.');
    const symbol = this.evaluatedCubes[cubeAndName[0]] &&
      this.evaluatedCubes[cubeAndName[0]][type] &&
      this.evaluatedCubes[cubeAndName[0]][type][cubeAndName[1]];
    return symbol !== undefined;
  }

  public byPathAnyType(path: string[]) {
    if (this.isInstanceOfType('measures', path)) {
      return this.byPath('measures', path);
    }

    if (this.isInstanceOfType('dimensions', path)) {
      return this.byPath('dimensions', path);
    }

    if (this.isInstanceOfType('segments', path)) {
      return this.byPath('segments', path);
    }

    throw new UserError(`Can't resolve member '${path.join('.')}'`);
  }

  public byPath(type: 'measures' | 'dimensions' | 'segments', path: string | string[]) {
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

  public parsePath(type, path) {
    // Should throw UserError in case of parse error
    this.byPath(type, path);
    return path.split('.');
  }

  public isRbacEnabledForCube(cube: any): boolean {
    return cube.accessPolicy && cube.accessPolicy.length;
  }

  public isRbacEnabled(): boolean {
    if (this.isRbacEnabledCache === null) {
      this.isRbacEnabledCache = this.cubeNames().some(
        cubeName => this.isRbacEnabledForCube(this.cubeFromPath(cubeName))
      );
    }
    return this.isRbacEnabledCache;
  }

  protected parsePathAnyType(path) {
    // Should throw UserError in case of parse error
    this.byPathAnyType(path);
    return path.split('.');
  }

  public collectUsedCubeReferences(cube: any, sqlFn: any) {
    const cubeEvaluator = this;

    const cubeReferencesUsed: string[] = [];
    const pathReferencesUsed: string[][] = [];

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
      sqlResolveFn: (_symbol: unknown, cubeName: string, memberName: string) => {
        const path = [cubeName, memberName];
        pathReferencesUsed.push(path);
        return cubeEvaluator.pathFromArray(path);
      },
      contextSymbols: BaseQuery.emptyParametrizedContextSymbols(this, () => '$empty_param$'),
      cubeReferencesUsed,
    });
    return { cubeReferencesUsed, pathReferencesUsed, evaluatedSql };
  }

  protected evaluatePreAggregationReferences(cube, aggregation) {
    const timeDimensions: any = [];

    if (aggregation.timeDimensionReference) {
      timeDimensions.push({
        dimension: this.evaluateReferences(cube, aggregation.timeDimensionReference),
        granularity: aggregation.granularity
      });
    } else if (aggregation.timeDimensionReferences) {
      // eslint-disable-next-line guard-for-in
      for (const timeDimensionReference of aggregation.timeDimensionReferences) {
        timeDimensions.push({
          dimension: this.evaluateReferences(cube, timeDimensionReference.dimension),
          granularity: timeDimensionReference.granularity
        });
      }
    }

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
