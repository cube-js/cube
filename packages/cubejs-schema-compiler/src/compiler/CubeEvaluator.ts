/* eslint-disable no-restricted-syntax */
import R from 'ramda';

import {
  AccessPolicyDefinition,
  CubeDefinitionExtended,
  CubeSymbols,
  HierarchyDefinition,
  JoinDefinition,
  PreAggregationDefinition,
  PreAggregationDefinitionRollup,
  type ToString
} from './CubeSymbols';
import { UserError } from './UserError';
import { BaseQuery, PreAggregationDefinitionExtended } from '../adapter';
import type { CubeValidator } from './CubeValidator';
import type { ErrorReporter } from './ErrorReporter';
import { FinishedJoinTree } from './JoinGraph';

export type SegmentDefinition = {
  type: string;
  sql(): string;
  primaryKey?: true;
  ownedByCube: boolean;
  fieldType?: string;
  // TODO should we have it here?
  multiStage?: boolean;
};

export type DimensionDefinition = {
  type: string;
  sql(): string;
  primaryKey?: true;
  ownedByCube: boolean;
  fieldType?: string;
  multiStage?: boolean;
  shiftInterval?: string;
};

export type TimeShiftDefinition = {
  timeDimension?: (...args: Array<unknown>) => ToString;
  name?: string;
  interval?: string;
  type?: 'next' | 'prior';
};

export type TimeShiftDefinitionReference = {
  timeDimension?: string;
  name?: string;
  interval?: string;
  type?: 'next' | 'prior';
};

export type MeasureDefinition = {
  type: string;
  sql(): string;
  ownedByCube: boolean;
  rollingWindow?: any
  filters?: any
  primaryKey?: true;
  drillFilters?: any;
  multiStage?: boolean;
  groupBy?: (...args: Array<unknown>) => Array<ToString>;
  reduceBy?: (...args: Array<unknown>) => Array<ToString>;
  addGroupBy?: (...args: Array<unknown>) => Array<ToString>;
  timeShift?: TimeShiftDefinition[];
  groupByReferences?: string[];
  reduceByReferences?: string[];
  addGroupByReferences?: string[];
  timeShiftReferences?: TimeShiftDefinitionReference[];
  patchedFrom?: { cubeName: string; name: string };
};

export type PreAggregationFilters = {
  dataSources?: string[];
  cubes?: string[];
  preAggregationIds?: string[];
  scheduled?: boolean;
};

export type PreAggregationDefinitions = Record<string, PreAggregationDefinition>;

export type PreAggregationTimeDimensionReference = {
  dimension: string,
  granularity: string,
};

/// Strings in `dimensions`, `measures` and `timeDimensions[*].dimension` can contain full join path, not just `cube.member`
export type PreAggregationReferences = {
  allowNonStrictDateRangeMatch?: boolean,
  dimensions: Array<string>,
  fullNameDimensions: Array<string>,
  measures: Array<string>,
  fullNameMeasures: Array<string>,
  timeDimensions: Array<PreAggregationTimeDimensionReference>,
  fullNameTimeDimensions: Array<PreAggregationTimeDimensionReference>,
  rollups: Array<string>,
  multipliedMeasures?: Array<string>,
  joinTree?: FinishedJoinTree;
};

export type PreAggregationInfo = {
  id: string,
  preAggregationName: string,
  preAggregation: unknown,
  cube: string,
  references: PreAggregationReferences,
  refreshKey: unknown,
  indexesReferences: unknown,
};

export type EvaluatedHierarchy = {
  name: string;
  title?: string;
  public?: boolean;
  levels: string[];
  aliasMember?: string;
  [key: string]: any;
};

export type EvaluatedFolder = {
  name: string;
  includes: (EvaluatedFolder | DimensionDefinition | MeasureDefinition)[];
  type: 'folder';
  [key: string]: any;
};

export type EvaluatedCube = {
  measures: Record<string, MeasureDefinition>;
  dimensions: Record<string, DimensionDefinition>;
  segments: Record<string, SegmentDefinition>;
  joins: JoinDefinition[];
  hierarchies: Record<string, HierarchyDefinition>;
  evaluatedHierarchies: EvaluatedHierarchy[];
  preAggregations: Record<string, PreAggregationDefinitionExtended>;
  dataSource?: string;
  folders: EvaluatedFolder[];
  sql?: (...args: any[]) => string;
  sqlTable?: (...args: any[]) => string;
  accessPolicy?: AccessPolicyDefinition[];
};

export class CubeEvaluator extends CubeSymbols {
  public evaluatedCubes: Record<string, EvaluatedCube> = {};

  public primaryKeys: Record<string, string[]> = {};

  public byFileName: Record<string, CubeDefinitionExtended[]> = {};

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

    this.byFileName = R.groupBy(v => v.fileName || v.name, validCubes);
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

  protected prepareCube(cube, errorReporter: ErrorReporter): EvaluatedCube {
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
    const folders = cube.rawFolders();
    if (!folders.length) return;

    const checkMember = (memberName: string, folderName: string) => {
      if (memberName.includes('.')) {
        errorReporter.error(
          `Paths aren't allowed in the 'folders' but '${memberName}' has been provided for ${cube.name}`
        );
      }

      const member = cube.includedMembers.find(m => m.name === memberName);
      if (!member) {
        errorReporter.error(
          `Member '${memberName}' included in folder '${folderName}' not found`
        );
        return null;
      }

      return member;
    };

    const processFolder = (folder: any): any => {
      let includedMembers: string[];
      let includes: any[] = [];

      if (folder.includes === '*') {
        includedMembers = this.allMembersOrList(cube, folder.includes);
        includes = includedMembers.map(m => checkMember(m, folder.name)).filter(Boolean);
      } else if (Array.isArray(folder.includes)) {
        includes = folder.includes.map(item => {
          if (typeof item === 'object' && item !== null) {
            return processFolder(item);
          }

          return checkMember(item, folder.name);
        });
      }

      return {
        ...folder,
        type: 'folder',
        includes: includes.filter(Boolean)
      };
    };

    cube.folders = folders.map(processFolder);
  }

  private prepareHierarchies(cube: any, errorReporter: ErrorReporter): void {
    // Hierarchies from views are not fully populated at this moment and are processed later,
    // so we should not pollute the cube hierarchies definition here.
    if (!cube.isView && Object.keys(cube.hierarchies).length) {
      cube.evaluatedHierarchies = Object.entries(cube.hierarchies).map(([name, hierarchy]) => ({
        name,
        ...(typeof hierarchy === 'object' ? hierarchy : {}),
        levels: this.evaluateReferences(
          cube.name,
          // @ts-ignore
          hierarchy.levels,
          { originalSorting: true }
        )
      }));
    }

    if (cube.isView && (cube.includedMembers || []).length) {
      const includedMemberPaths: string[] = R.uniq(cube.includedMembers.map(it => it.memberPath));
      const includedCubeNames: string[] = R.uniq(includedMemberPaths.map(it => it.split('.')[0]));

      // Path to name (which can be prefixed or aliased) map for hierarchy
      const hierarchyPathToName = cube.includedMembers.filter(it => it.type === 'hierarchies').reduce((acc, it) => ({
        ...acc,
        [it.memberPath]: it.name
      }), {});
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
              })
                .filter(Boolean);

              const aliasMember = [cubeName, it.name].join('.');

              const name = hierarchyPathToName[aliasMember];
              if (!name) {
                throw new UserError(`Hierarchy '${it.name}' not found in cube '${cubeName}'`);
              }

              return {
                // Title might be overridden in the view
                title: cube.hierarchies?.[it.name]?.override?.title || it.title,
                ...it,
                aliasMember,
                name,
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
          member.timeShiftReferences = member.timeShift.map((s): TimeShiftDefinitionReference => ({
            name: s.name,
            interval: s.interval,
            type: s.type,
            ...(typeof s.timeDimension === 'function'
              ? { timeDimension: this.evaluateReferences(cubeName, s.timeDimension) }
              : {}),
          }));
        }
      }
    }
  }

  protected prepareJoins(cube: any, errorReporter: ErrorReporter) {
    if (!cube.joins) {
      return;
    }

    const transformRelationship = (relationship: string): string => {
      switch (relationship) {
        case 'belongs_to':
        case 'many_to_one':
        case 'manyToOne':
          return 'belongsTo';
        case 'has_many':
        case 'one_to_many':
        case 'oneToMany':
          return 'hasMany';
        case 'has_one':
        case 'one_to_one':
        case 'oneToOne':
          return 'hasOne';
        default:
          return relationship;
      }
    };

    let joins: JoinDefinition[] = [];

    if (Array.isArray(cube.joins)) {
      joins = cube.joins.map((join: JoinDefinition) => {
        join.relationship = transformRelationship(join.relationship);
        return join;
      });
    } else if (typeof cube.joins === 'object') {
      joins = Object.entries(cube.joins).map(([name, join]: [string, any]) => {
        join.relationship = transformRelationship(join.relationship);
        join.name = name;
        return join as JoinDefinition;
      });
    } else {
      errorReporter.error(`Invalid joins definition for cube '${cube.name}': expected an array or an object.`);
    }

    cube.joins = joins;
  }

  protected preparePreAggregations(cube: any, errorReporter: ErrorReporter) {
    if (cube.preAggregations) {
      // eslint-disable-next-line no-restricted-syntax
      for (const preAggregation of Object.values(cube.preAggregations) as any) {
        // preAggregation is actually (PreAggregationDefinitionRollup | PreAggregationDefinitionOriginalSql)
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
        // We won't check for FILTER_PARAMS here as it shouldn't affect ownership, and it should obey the same reference rules.
        // To affect ownership FILTER_PARAMS can be declared as `${FILTER_PARAMS.Foo.bar.filter(`${Foo.bar}`)}`.
        // It isn't owned if there are non {CUBE} references
        if (funcArgs.length > 0 && cubeReferencesUsed.length === 0) {
          ownedByCube = false;
        }
        // Aliases one to one some another member as in case of views
        // Note: Segments do not have type set
        if (!ownedByCube && !member.filters && (!member.type || CubeSymbols.isCalculatedMeasureType(member.type)) && pathReferencesUsed.length === 1 && this.pathFromArray(pathReferencesUsed[0]) === evaluatedSql) {
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

  public cubesByFileName(fileName): CubeDefinitionExtended[] {
    return this.byFileName[fileName] || [];
  }

  public timeDimensionPathsForCube(cube: string): string[] {
    return R.compose(
      R.map(dimName => `${cube}.${dimName}`),
      R.keys,
      // @ts-ignore
      R.filter((d: any) => d.type === 'time')
      // @ts-ignore
    )(this.evaluatedCubes[cube].dimensions || {});
  }

  public measuresForCube(cube: string): Record<string, MeasureDefinition> {
    return this.cubeFromPath(cube).measures || {};
  }

  public timeDimensionsForCube(cube: string): Record<string, DimensionDefinition> {
    return R.filter(
      (d: any) => d.type === 'time',
      this.cubeFromPath(cube).dimensions || {}
    );
  }

  public preAggregationsForCube(path: string): Record<string, PreAggregationDefinitionExtended> {
    return this.cubeFromPath(path).preAggregations || {};
  }

  public preAggregationsForCubeAsArray(path: string) {
    return Object.entries(this.cubeFromPath(path).preAggregations || {}).map(([name, preAggregation]) => ({
      name,
      ...(preAggregation as Record<string, any>)
    }));
  }

  public preAggregationDescriptionByName(cubeName: string, preAggName: string) {
    const cube = this.cubeFromPath(cubeName);
    const preAggregations = cube.preAggregations || {};

    const preAgg = preAggregations[preAggName];

    if (!preAgg) {
      return undefined;
    }

    return {
      name: preAggName,
      ...(preAgg as Record<string, any>)
    };
  }

  /**
   * Returns pre-aggregations filtered by the specified selector.
   */
  public preAggregations(filter: PreAggregationFilters): Array<PreAggregationInfo> {
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
      .flatMap(cube => {
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
      });
  }

  public scheduledPreAggregations(): Array<PreAggregationInfo> {
    return this.preAggregations({ scheduled: true });
  }

  public cubeNames(): string[] {
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
    return this.byPath('measures', measurePath) as MeasureDefinition;
  }

  public dimensionByPath(dimensionPath: string): DimensionDefinition {
    return this.byPath('dimensions', dimensionPath) as DimensionDefinition;
  }

  public segmentByPath(segmentPath: string): SegmentDefinition {
    return this.byPath('segments', segmentPath) as SegmentDefinition;
  }

  public cubeExists(cube: string): boolean {
    return !!this.evaluatedCubes[cube];
  }

  public cubeFromPath(path: string): EvaluatedCube {
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
    const symbol = this.evaluatedCubes[cubeAndName[0]]?.[type]?.[cubeAndName[1]];
    return symbol !== undefined;
  }

  public byPathAnyType(path: string | string[]) {
    if (this.isInstanceOfType('measures', path)) {
      return this.byPath('measures', path);
    }

    if (this.isInstanceOfType('dimensions', path)) {
      return this.byPath('dimensions', path);
    }

    if (this.isInstanceOfType('segments', path)) {
      return this.byPath('segments', path);
    }

    throw new UserError(`Can't resolve member '${Array.isArray(path) ? path.join('.') : path}'`);
  }

  public byPath<T extends 'measures' | 'dimensions' | 'segments' | 'preAggregations'>(type: T, path: string | string[]): EvaluatedCube[T][string] {
    if (!type) {
      throw new Error(`Type can't be undefined for '${path}'`);
    }

    if (!path) {
      throw new Error('Path can\'t be undefined');
    }

    const cubeAndName = Array.isArray(path) ? path : path.split('.');
    const cube = this.evaluatedCubes[cubeAndName[0]];
    if (cube === undefined) {
      throw new UserError(`Cube '${cubeAndName[0]}' not found for path '${path}'`);
    }

    const typeMembers = cube[type];
    if (typeMembers === undefined) {
      throw new UserError(`${type} not defined for path '${path}'`);
    }

    const member = typeMembers[cubeAndName[1]];
    if (member === undefined) {
      throw new UserError(`'${cubeAndName[1]}' not found for path '${path}'`);
    }

    return member as EvaluatedCube[T][string];
  }

  public parsePath(type: 'measures' | 'dimensions' | 'segments' | 'preAggregations', path: string): string[] {
    // Should throw UserError in case of parse error
    this.byPath(type, path);
    return path.split('.');
  }

  public isRbacEnabledForCube(cube: any): boolean {
    return cube.accessPolicy?.length;
  }

  public isRbacEnabled(): boolean {
    if (this.isRbacEnabledCache === null) {
      this.isRbacEnabledCache = this.cubeNames().some(
        cubeName => this.isRbacEnabledForCube(this.cubeFromPath(cubeName))
      );
    }
    return this.isRbacEnabledCache;
  }

  public parsePathAnyType(path: string): string[] {
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

  /**
   * Evaluates rollup references for retrieving rollupReference used in Tesseract.
   * This is a temporary solution until Tesseract takes ownership of all pre-aggregations.
   */
  public evaluateRollupReferences<T extends ToString | Array<ToString>>(cube: string, rollupReferences: (...args: Array<unknown>) => T) {
    return this.evaluateReferences(cube, rollupReferences, { originalSorting: true });
  }

  public evaluatePreAggregationReferences(cube: string, aggregation: PreAggregationDefinitionRollup): PreAggregationReferences {
    const timeDimensions: Array<PreAggregationTimeDimensionReference> = [];

    if (aggregation.timeDimensionReference) {
      timeDimensions.push({
        dimension: this.evaluateReferences(cube, aggregation.timeDimensionReference, { collectJoinHints: true }),
        granularity: aggregation.granularity
      });
    } else if (aggregation.timeDimensionReferences) {
      // eslint-disable-next-line guard-for-in
      for (const timeDimensionReference of aggregation.timeDimensionReferences) {
        timeDimensions.push({
          dimension: this.evaluateReferences(cube, timeDimensionReference.dimension, { collectJoinHints: true }),
          granularity: timeDimensionReference.granularity
        });
      }
    }

    return {
      allowNonStrictDateRangeMatch: aggregation.allowNonStrictDateRangeMatch,
      dimensions:
        (aggregation.dimensionReferences && this.evaluateReferences(cube, aggregation.dimensionReferences, { collectJoinHints: true }) || [])
          .concat(
            aggregation.segmentReferences && this.evaluateReferences(cube, aggregation.segmentReferences, { collectJoinHints: true }) || []
          ),
      measures:
        (aggregation.measureReferences && this.evaluateReferences(cube, aggregation.measureReferences, { collectJoinHints: true }) || []),
      timeDimensions,
      rollups:
        aggregation.rollupReferences && this.evaluateReferences(cube, aggregation.rollupReferences, { originalSorting: true }) || [],
      fullNameDimensions: [], // May be filled in PreAggregations.evaluateAllReferences()
      fullNameMeasures: [], // May be filled in PreAggregations.evaluateAllReferences()
      fullNameTimeDimensions: [], // May be filled in PreAggregations.evaluateAllReferences()
    };
  }
}
