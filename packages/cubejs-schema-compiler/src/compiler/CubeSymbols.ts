import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';
import { camelize } from 'inflection';

import { UserError } from './UserError';
import { DynamicReference } from './DynamicReference';
import { camelizeCube } from './utils';

import type { ErrorReporter } from './ErrorReporter';

export type ToString = { toString(): string };

export type GranularityDefinition = {
  sql?: (...args: any[]) => string;
  title?: string;
  interval?: string;
  offset?: string;
  origin?: string;
};

export type TimeshiftDefinition = {
  interval?: string;
  type?: string;
  name?: string;
  timeDimension?: (...args: any[]) => string;
};

export type CubeSymbolDefinition = {
  type?: string;
  sql?: (...args: any[]) => string;
  primaryKey?: boolean;
  granularities?: Record<string, GranularityDefinition>;
  timeShift?: TimeshiftDefinition[];
  format?: string;
};

export type HierarchyDefinition = {
  title?: string;
  public?: boolean;
  levels?: (...args: any[]) => string[];
};

export type EveryInterval = string;
type EveryCronInterval = string;
type EveryCronTimeZone = string;

export type CubeRefreshKeySqlVariant = {
  sql: () => string;
  every?: EveryInterval;
};

export type CubeRefreshKeyEveryVariant = {
  every: EveryInterval | EveryCronInterval;
  timezone?: EveryCronTimeZone;
  incremental?: boolean;
  updateWindow?: EveryInterval;
};

export type CubeRefreshKeyImmutableVariant = {
  immutable: true;
};

export type CubeRefreshKey =
  | CubeRefreshKeySqlVariant
  | CubeRefreshKeyEveryVariant
  | CubeRefreshKeyImmutableVariant;

type BasePreAggregationDefinition = {
  allowNonStrictDateRangeMatch?: boolean;
  useOriginalSqlPreAggregations?: boolean;
  timeDimensionReference?: (...args: any[]) => ToString;
  indexes?: Record<string, any>;
  refreshKey?: CubeRefreshKey;
  ownedByCube?: boolean;
};

export type PreAggregationDefinitionOriginalSql = BasePreAggregationDefinition & {
  type: 'originalSql';
  partitionGranularity?: string;
  // eslint-disable-next-line camelcase
  partition_granularity?: string;
  // eslint-disable-next-line camelcase
  time_dimension?: (...args: any[]) => ToString;
};

export type PreAggregationDefinitionRollup = BasePreAggregationDefinition & {
  type: 'autoRollup' | 'rollupJoin' | 'rollupLambda' | 'rollup';
  granularity: string;
  timeDimensionReferences: Array<{ dimension: () => ToString; granularity: string }>;
  dimensionReferences: (...args: any[]) => ToString[];
  segmentReferences: (...args: any[]) => ToString[];
  measureReferences: (...args: any[]) => ToString[];
  rollupReferences: (...args: any[]) => ToString[];
  scheduledRefresh: boolean;
  external: boolean;
};

// PreAggregationDefinition is widely used in the codebase, but it's assumed to be rollup,
// originalSql is not refreshed and so on.
export type PreAggregationDefinition = PreAggregationDefinitionRollup;

export type JoinDefinition = {
  name: string,
  relationship: string,
  sql: (...args: any[]) => string,
};

export type Filter =
  | {
      member: string;
      memberReference?: string;
      [key: string]: any;
    }
  | {
      and?: Filter[];
      or?: Filter[];
      [key: string]: any;
    };

export type AccessPolicyDefinition = {
  role?: string;
  group?: string;
  groups?: string[];
  rowLevel?: {
    filters: Filter[];
  };
  memberLevel?: {
    includes?: string | string[];
    excludes?: string | string[];
    includesMembers?: string[];
    excludesMembers?: string[];
  };
};

export interface CubeDefinition {
  name: string;
  extends?: (...args: Array<unknown>) => { __cubeName: string };
  sql?: string | ((...args: any[]) => string);
  // eslint-disable-next-line camelcase
  sql_table?: string | ((...args: any[]) => string);
  sqlTable?: string | ((...args: any[]) => string);
  dataSource?: string;
  measures?: Record<string, CubeSymbolDefinition>;
  dimensions?: Record<string, CubeSymbolDefinition>;
  segments?: Record<string, CubeSymbolDefinition>;
  hierarchies?: Record<string, HierarchyDefinition>;
  preAggregations?: Record<string, PreAggregationDefinitionRollup | PreAggregationDefinitionOriginalSql>;
  // eslint-disable-next-line camelcase
  pre_aggregations?: Record<string, PreAggregationDefinitionRollup | PreAggregationDefinitionOriginalSql>;
  joins?: JoinDefinition[];
  accessPolicy?: AccessPolicyDefinition[];
  // eslint-disable-next-line camelcase
  access_policy?: any[];
  folders?: any[];
  includes?: any;
  excludes?: any;
  cubes?: any;
  isView?: boolean;
  calendar?: boolean;
  isSplitView?: boolean;
  includedMembers?: any[];
  fileName?: string;
}

export interface CubeDefinitionExtended extends CubeDefinition {
  allDefinitions: (type: string) => Record<string, any>;
  rawFolders: () => any[];
  rawCubes: () => any[];
}

interface SplitViews {
  [key: string]: any;
}

export interface CubeSymbolsBase {
  cubeName: () => string;
  cubeObj: () => CubeDefinitionExtended;
}

export type CubeSymbolsDefinition = CubeSymbolsBase & Record<string, CubeSymbolDefinition>;

const FunctionRegex = /function\s+\w+\(([A-Za-z0-9_,]*)|\(([\s\S]*?)\)\s*=>|\(?(\w+)\)?\s*=>/;
export const CONTEXT_SYMBOLS = {
  SECURITY_CONTEXT: 'securityContext',
  // SECURITY_CONTEXT has been deprecated, however security_context (lowercase)
  // is allowed in RBAC policies for query-time attribute matching
  security_context: 'securityContext',
  securityContext: 'securityContext',
  FILTER_PARAMS: 'filterParams',
  FILTER_GROUP: 'filterGroup',
  SQL_UTILS: 'sqlUtils'
};

export const CURRENT_CUBE_CONSTANTS = ['CUBE', 'TABLE'];

export class CubeSymbols {
  public symbols: Record<string | symbol, CubeSymbolsDefinition>;

  private builtCubes: Record<string, CubeDefinitionExtended>;

  private cubeDefinitions: Record<string, CubeDefinition>;

  private funcArgumentsValues: Record<string, string[]>;

  public cubeList: CubeDefinitionExtended[];

  private readonly evaluateViews: boolean;

  private resolveSymbolsCallContext: any;

  public constructor(evaluateViews = false) {
    this.symbols = {};
    this.builtCubes = {};
    this.cubeDefinitions = {};
    this.funcArgumentsValues = {};
    this.cubeList = [];
    this.evaluateViews = evaluateViews;
  }

  public free() {
    this.symbols = {};
    this.builtCubes = {};
    this.cubeDefinitions = {};
    this.funcArgumentsValues = {};
    this.cubeList = [];
    this.resolveSymbolsCallContext = undefined;
  }

  public compile(cubes: CubeDefinition[], errorReporter: ErrorReporter) {
    this.cubeDefinitions = Object.fromEntries(
      cubes.map((c): [string, CubeDefinition] => [c.name, c])
    );

    this.cubeList = cubes.map(c => (c.name ? this.getCubeDefinition(c.name) : this.createCube(c)));
    // TODO support actual dependency sorting to allow using views inside views
    const sortedByDependency = R.pipe(
      R.sortBy((c: CubeDefinition) => !!c.isView),
    )(cubes);
    for (const cube of sortedByDependency) {
      const splitViews: SplitViews = {};
      this.symbols[cube.name] = this.transform(cube.name, errorReporter.inContext(`${cube.name} cube`), splitViews);
      for (const viewName of Object.keys(splitViews)) {
        // TODO can we define it when cubeList is defined?
        this.cubeList.push(splitViews[viewName]);
        this.symbols[viewName] = splitViews[viewName];
        this.cubeDefinitions[viewName] = splitViews[viewName];
      }
    }
  }

  public getCubeDefinition(cubeName: string): CubeDefinitionExtended {
    if (!this.builtCubes[cubeName]) {
      const cubeDefinition = this.cubeDefinitions[cubeName];
      this.builtCubes[cubeName] = this.createCube(cubeDefinition);
    }

    return this.builtCubes[cubeName];
  }

  public createCube(cubeDefinition: CubeDefinition): CubeDefinitionExtended {
    let preAggregations: CubeDefinition['preAggregations'];
    let joins: CubeDefinition['joins'];
    let measures: CubeDefinition['measures'];
    let dimensions: CubeDefinition['dimensions'];
    let segments: CubeDefinition['segments'];
    let hierarchies: CubeDefinition['hierarchies'];
    let accessPolicy: CubeDefinition['accessPolicy'];
    let folders: CubeDefinition['folders'];
    let cubes: CubeDefinition['cubes'];

    const cubeObject: CubeDefinitionExtended = Object.assign({
      allDefinitions(type: string) {
        if (cubeDefinition.extends) {
          return {
            ...super.allDefinitions(type),
            ...cubeDefinition[type]
          };
        } else {
          return { ...cubeDefinition[type] };
        }
      },

      // Folders are not a part of Cube Symbols and are constructed in the CubeEvaluator,
      // but views can extend other views, so we need the ability to access parent's folders.
      rawFolders() {
        if (!folders) {
          if (cubeDefinition.extends) {
            folders = [
              ...super.rawFolders(),
              ...(cubeDefinition.folders || [])
            ];
          } else {
            folders = [...(cubeDefinition.folders || [])];
          }
        }
        return folders;
      },

      // `Cubes` of a view are not a part of Cube Symbols,
      // but views can extend other views, so we need the ability to access parent view's cubes.
      rawCubes() {
        if (!cubes) {
          if (cubeDefinition.extends) {
            cubes = [
              ...super.rawCubes(),
              ...(cubeDefinition.cubes || [])
            ];
          } else {
            cubes = [...(cubeDefinition.cubes || [])];
          }
        }
        return cubes;
      },

      get preAggregations() {
        // For preAggregations order is important, and destructing parents cube pre-aggs first will lead to
        // unexpected results, so we can not use common approach with allDefinitions('preAggregations') here.
        if (!preAggregations) {
          const parentPreAggregations = cubeDefinition.extends ? super.preAggregations : null;
          // Unfortunately, cube is not camelized yet at this point :(
          const localPreAggregations = cubeDefinition.preAggregations || cubeDefinition.pre_aggregations;

          if (parentPreAggregations) {
            preAggregations = { ...localPreAggregations, ...parentPreAggregations, ...localPreAggregations };
          } else {
            preAggregations = { ...localPreAggregations };
          }
        }
        return preAggregations;
      },
      set preAggregations(_v) {
        // Dont allow to modify
      },

      get joins() {
        if (!joins) {
          // In dynamic models we still can hit the cases where joins are returned as map
          // instead of array, so we need to convert them here to array.
          // TODO: Simplify/Remove this when we drop map joins support totally.
          let parentJoins = cubeDefinition.extends ? super.joins : [];
          if (!Array.isArray(parentJoins)) {
            parentJoins = Object.entries(parentJoins).map(([name, join]: [string, any]) => {
              join.name = name;
              return join as JoinDefinition;
            });
          }

          let localJoins = cubeDefinition.joins || [];
          // TODO: Simplify/Remove this when we drop map joins support totally.
          if (!Array.isArray(localJoins)) {
            localJoins = Object.entries(localJoins).map(([name, join]: [string, any]) => {
              join.name = name;
              return join as JoinDefinition;
            });
          }

          joins = [...parentJoins, ...localJoins];
        }
        return joins;
      },
      set joins(_v) {
        // Dont allow to modify
      },

      get measures() {
        if (!measures) {
          measures = this.allDefinitions('measures');
        }
        return measures;
      },
      set measures(_v) {
        // Dont allow to modify
      },

      get dimensions() {
        if (!dimensions) {
          dimensions = this.allDefinitions('dimensions');
        }
        return dimensions;
      },
      set dimensions(_v) {
        // Dont allow to modify
      },

      get segments() {
        if (!segments) {
          segments = this.allDefinitions('segments');
        }
        return segments;
      },
      set segments(_v) {
        // Dont allow to modify
      },

      get hierarchies() {
        if (!hierarchies) {
          hierarchies = this.allDefinitions('hierarchies');
        }
        return hierarchies;
      },
      set hierarchies(_v) {
        // Dont allow to modify
      },

      get accessPolicy() {
        if (!accessPolicy) {
          const parentAcls = cubeDefinition.extends ? R.clone(super.accessPolicy) : [];
          const localAccessPolicy = cubeDefinition.accessPolicy || cubeDefinition.access_policy;
          accessPolicy = [...(parentAcls || []), ...(localAccessPolicy || [])];
        }
        // Schema validator expects accessPolicy to be not empty if defined
        if (accessPolicy.length) {
          return accessPolicy;
        } else {
          return undefined;
        }
      },
      set accessPolicy(_v) {
        // Dont allow to modify
      }
    },
    cubeDefinition);

    if (cubeDefinition.extends) {
      const superCube = this.resolveSymbolsCall(cubeDefinition.extends, (name: string) => this.cubeReferenceProxy(name));
      // eslint-disable-next-line no-underscore-dangle
      const parentCube = superCube.__cubeName ? this.getCubeDefinition(superCube.__cubeName) : superCube as unknown as CubeDefinition;
      Object.setPrototypeOf(cubeObject, parentCube);

      // We have 2 different properties that are mutually exclusive: `sqlTable` & `sql`
      // And if in extending cube one of them is defined - we need to hide the other from parent cube definition
      // Unfortunately, cube is not camelized yet at this point :(
      if ((cubeDefinition.sqlTable || cubeDefinition.sql_table) && parentCube.sql) {
        cubeObject.sql = undefined;
      } else if (cubeDefinition.sql && (parentCube.sqlTable || parentCube.sql_table)) {
        cubeObject.sqlTable = undefined;
      }
    }

    return cubeObject;
  }

  protected transform(cubeName: string, errorReporter: ErrorReporter, splitViews: SplitViews): CubeSymbolsDefinition {
    const cube = this.getCubeDefinition(cubeName);
    // @ts-ignore
    const duplicateNames: string[] = R.compose(
      R.map((nameToDefinitions: any) => nameToDefinitions[0]),
      R.toPairs,
      R.filter((definitionsByName: any) => definitionsByName.length > 1),
      R.groupBy((nameToDefinition: any) => nameToDefinition[0]),
      R.unnest,
      R.map(R.toPairs),
      // @ts-ignore
      R.filter((v: any) => !!v)
      // @ts-ignore
    )([cube.measures, cube.dimensions, cube.segments, cube.preAggregations, cube.hierarchies]);

    if (duplicateNames.length > 0) {
      errorReporter.error(`${duplicateNames.join(', ')} defined more than once`);
    }

    camelizeCube(cube);

    this.camelCaseTypes(cube.joins);
    this.camelCaseTypes(cube.measures);
    this.camelCaseTypes(cube.dimensions);
    this.camelCaseTypes(cube.segments);
    this.camelCaseTypes(cube.preAggregations);
    this.camelCaseTypes(cube.accessPolicy);

    if (cube.preAggregations) {
      this.transformPreAggregations(cube.preAggregations);
    }

    if (this.evaluateViews) {
      this.prepareIncludes(cube, errorReporter, splitViews);
    }

    return {
      cubeName: () => cube.name,
      cubeObj: () => cube,
      ...cube.measures || {},
      ...cube.dimensions || {},
      ...cube.segments || {},
      ...cube.preAggregations || {}
    } as CubeSymbolsDefinition;
  }

  private camelCaseTypes(obj: Object | Array<any> | undefined) {
    if (!obj) {
      return;
    }

    const members = Array.isArray(obj) ? obj : Object.values(obj);

    members.forEach(member => {
      if (member.type && member.type.indexOf('_') !== -1) {
        member.type = camelize(member.type, true);
      }
      if (member.relationship && member.relationship.indexOf('_') !== -1) {
        member.relationship = camelize(member.relationship, true);
      }
    });
  }

  protected transformPreAggregations(preAggregations: Object) {
    // eslint-disable-next-line no-restricted-syntax
    for (const preAggregation of Object.values(preAggregations)) {
      // We don't want to set the defaults for the empty pre-aggs because
      // we want to throw instead.
      if (Object.keys(preAggregation).length > 0) {
        // Rollup is a default type for pre-aggregations
        if (!preAggregation.type) {
          preAggregation.type = 'rollup';
        }

        if (preAggregation.allowNonStrictDateRangeMatch === undefined &&
          ['originalSql', 'rollupJoin', 'rollup'].includes(preAggregation.type) &&
          (preAggregation.timeDimension || preAggregation.timeDimensions)) {
          preAggregation.allowNonStrictDateRangeMatch = getEnv('allowNonStrictDateRangeMatching');
        }

        if (preAggregation.scheduledRefresh === undefined && preAggregation.type !== 'rollupJoin' && preAggregation.type !== 'rollupLambda') {
          preAggregation.scheduledRefresh = getEnv('scheduledRefreshDefault');
        }

        if (preAggregation.external === undefined && preAggregation.type !== 'rollupLambda') {
          preAggregation.external =
            // TODO remove rollupJoin from this list and update validation
            ['rollup', 'rollupJoin'].includes(preAggregation.type) &&
            getEnv('externalDefault');
        }

        if (preAggregation.indexes) {
          this.transformPreAggregationIndexes(preAggregation.indexes);
        }
      }
    }
  }

  protected transformPreAggregationIndexes(indexes: Object) {
    for (const index of Object.values(indexes)) {
      if (!index.type) {
        index.type = 'regular';
      }
    }
  }

  protected prepareIncludes(cube: CubeDefinitionExtended, errorReporter: ErrorReporter, splitViews: SplitViews) {
    const includedCubes = cube.rawCubes();
    if (!includedCubes.length) {
      return;
    }

    const memberSets = {
      resolvedMembers: new Set<string>(),
      allMembers: new Set<string>(),
    };

    const autoIncludeMembers = new Set<string>();
    // `hierarchies` must be processed first
    const types = ['hierarchies', 'measures', 'dimensions', 'segments'];

    for (const type of types) {
      let cubeIncludes: any[] = [];

      // If the hierarchy is included all members from it should be included as well
      // Extend `includes` with members from hierarchies that should be auto-included
      const cubes = type === 'dimensions' ? includedCubes.map((it) => {
        // TODO recheck `it.joinPath` typing
        const fullPath = this.evaluateReferences(null, it.joinPath as () => ToString, { collectJoinHints: true });
        const split = fullPath.split('.');
        const cubeRef = split[split.length - 1];

        if (it.includes === '*') {
          return it;
        }

        const currentCubeAutoIncludeMembers = Array.from(autoIncludeMembers)
          .filter((path) => path.startsWith(`${cubeRef}.`))
          .map((path) => path.split('.')[1])
          .filter(memberName => !it.includes.find((include) => (include.name || include) === memberName));

        return {
          ...it,
          includes: (it.includes || []).concat(currentCubeAutoIncludeMembers),
        };
      }) : includedCubes;

      cubeIncludes = this.membersFromCubes(cube, cubes, type, errorReporter, splitViews, memberSets) || [];

      if (type === 'hierarchies') {
        for (const member of cubeIncludes) {
          const path = member.member.split('.');
          const cubeName = path[path.length - 2];
          const hierarchyName = path[path.length - 1];
          const hierarchy = this.getResolvedMember(type, cubeName, hierarchyName);

          if (hierarchy) {
            // TODO recheck `this.getResolvedMember(...).levels` typing
            const levels = this.evaluateReferences(cubeName, this.getResolvedMember('hierarchies', cubeName, hierarchyName).levels as () => Array<ToString>, { originalSorting: true });

            levels.forEach((level) => autoIncludeMembers.add(level));
          }
        }
      }

      const includeMembers = this.generateIncludeMembers(cubeIncludes, type);
      this.applyIncludeMembers(includeMembers, cube, type, errorReporter);

      const existing = cube.includedMembers ?? [];
      const seen = new Set(
        existing.map(({ type: t, memberPath, name }) => `${t}|${memberPath}|${name}`)
      );

      const additions: {
        type: string;
        memberPath: string;
        name: string;
      }[] = [];

      for (const { member, name } of cubeIncludes) {
        const parts = member.split('.');
        const memberPath = this.pathFromArray(parts.slice(-2));
        const key = `${type}|${memberPath}|${name}`;

        if (!seen.has(key)) {
          seen.add(key);
          additions.push({ type, memberPath, name });
        }
      }

      if (additions.length) {
        cube.includedMembers = [...existing, ...additions];
      }
    }

    [...memberSets.allMembers].filter(it => !memberSets.resolvedMembers.has(it)).forEach(it => {
      errorReporter.error(`Member '${it}' is included in '${cube.name}' but not defined in any cube`);
    });
  }

  protected applyIncludeMembers(includeMembers: any[], cube: CubeDefinition, type: string, errorReporter: ErrorReporter) {
    for (const [memberName, memberDefinition] of includeMembers) {
      if (cube[type]?.[memberName]) {
        errorReporter.error(`Included member '${memberName}' conflicts with existing member of '${cube.name}'. Please consider excluding this member or assigning it an alias.`);
      } else {
        cube[type][memberName] = memberDefinition;
      }
    }
  }

  protected membersFromCubes(
    parentCube: CubeDefinition,
    cubes: any[],
    type: string,
    errorReporter: ErrorReporter,
    splitViews: SplitViews,
    memberSets: any
  ) {
    const result: any[] = [];
    const seen = new Set<string>();

    for (const cubeInclude of cubes) {
      const fullPath = this.evaluateReferences(
        null,
        // TODO recheck `cubeInclude.joinPath` typing
        cubeInclude.joinPath as () => ToString,
        { collectJoinHints: true }
      );

      const split = fullPath.split('.');
      const cubeReference = split[split.length - 1];
      const cubeName = cubeInclude.alias || cubeReference;

      const fullMemberName = (memberName: string) => (cubeInclude.prefix ? `${cubeName}_${memberName}` : memberName);

      let includes: any[];

      if (cubeInclude.includes === '*') {
        const membersObj = this.symbols[cubeReference]?.cubeObj()?.[type] || {};
        includes = Object.keys(membersObj).map((memberName) => ({
          member: `${fullPath}.${memberName}`,
          name: fullMemberName(memberName),
        }));
      } else {
        includes = cubeInclude.includes.map((include: any) => {
          const member = include.alias || include.name || include;

          if (member.includes('.')) {
            errorReporter.error(
              `Paths aren't allowed in cube includes but '${member}' provided as include member`
            );
          }

          const name = fullMemberName(member);
          memberSets.allMembers.add(name);

          const includedMemberName = include.name || include;

          const resolved = this.getResolvedMember(
            type,
            cubeReference,
            includedMemberName
          );

          if (!resolved) return undefined;

          memberSets.resolvedMembers.add(name);

          const override = (include.title || include.description || include.format || include.meta)
            ? {
              title: include.title,
              description: include.description,
              format: include.format,
              meta: include.meta,
            }
            : undefined;

          return {
            member: `${fullPath}.${includedMemberName}`,
            name,
            ...(override ? { override } : {}),
          };
        });
      }

      const excludes = (cubeInclude.excludes || [])
        .map((exclude: any) => {
          if (exclude.includes('.')) {
            errorReporter.error(
              `Paths aren't allowed in cube excludes but '${exclude}' provided as exclude member`
            );
          }

          const resolved = this.getResolvedMember(type, cubeReference, exclude);
          return resolved ? { member: `${fullPath}.${exclude}` } : undefined;
        })
        .filter(Boolean);

      const finalIncludes = this.diffByMember(
        includes.filter(Boolean),
        excludes
      );

      if (cubeInclude.split) {
        const viewName = `${parentCube.name}_${cubeName}`;
        let splitViewDef = splitViews[viewName];
        if (!splitViewDef) {
          splitViews[viewName] = this.createCube({
            name: viewName,
            isView: true,
            // TODO might worth adding to validation as it goes around it right now
            isSplitView: true,
          });
          splitViewDef = splitViews[viewName];
        }

        const includeMembers = this.generateIncludeMembers(finalIncludes, type);
        this.applyIncludeMembers(includeMembers, splitViewDef, type, errorReporter);
      } else {
        for (const member of finalIncludes) {
          const key = `${member.member}|${member.name}`;
          if (!seen.has(key)) {
            seen.add(key);
            result.push(member);
          }
        }
      }
    }

    return result;
  }

  protected diffByMember(includes: any[], excludes: any[]) {
    const excludesMap = new Map();

    for (const exclude of excludes) {
      excludesMap.set(exclude.member, true);
    }

    return includes.filter(include => !excludesMap.has(include.member));
  }

  protected getResolvedMember(type: string, cubeName: string, memberName: string) {
    return this.symbols[cubeName]?.cubeObj()?.[type]?.[memberName];
  }

  protected generateIncludeMembers(members: any[], type: string) {
    return members.map(memberRef => {
      const path = memberRef.member.split('.');
      const resolvedMember = this.getResolvedMember(type, path[path.length - 2], path[path.length - 1]);
      if (!resolvedMember) {
        throw new Error(`Can't resolve '${memberRef.member}' while generating include members`);
      }

      // eslint-disable-next-line no-new-func
      const sql = new Function(path[0], `return \`\${${memberRef.member}}\`;`);
      let memberDefinition;
      if (type === 'measures') {
        memberDefinition = {
          sql,
          type: CubeSymbols.toMemberDataType(resolvedMember.type),
          aggType: resolvedMember.type,
          meta: memberRef.override?.meta || resolvedMember.meta,
          title: memberRef.override?.title || resolvedMember.title,
          description: memberRef.override?.description || resolvedMember.description,
          format: memberRef.override?.format || resolvedMember.format,
          ...(resolvedMember.multiStage && { multiStage: resolvedMember.multiStage }),
          ...(resolvedMember.timeShift && { timeShift: resolvedMember.timeShift }),
          ...(resolvedMember.orderBy && { orderBy: resolvedMember.orderBy }),
        };
      } else if (type === 'dimensions') {
        memberDefinition = {
          sql,
          type: resolvedMember.type,
          meta: memberRef.override?.meta || resolvedMember.meta,
          title: memberRef.override?.title || resolvedMember.title,
          description: memberRef.override?.description || resolvedMember.description,
          format: memberRef.override?.format || resolvedMember.format,
          ...(resolvedMember.granularities ? { granularities: resolvedMember.granularities } : {}),
          ...(resolvedMember.multiStage && { multiStage: resolvedMember.multiStage }),
        };
      } else if (type === 'segments') {
        memberDefinition = {
          sql,
          meta: memberRef.override?.meta || resolvedMember.meta,
          description: memberRef.override?.description || resolvedMember.description,
          title: memberRef.override?.title || resolvedMember.title,
          aliases: resolvedMember.aliases,
        };
      } else if (type === 'hierarchies') {
        memberDefinition = {
          title: memberRef.override?.title || resolvedMember.title,
          levels: resolvedMember.levels,
        };
      } else {
        throw new Error(`Unexpected member type: ${type}`);
      }
      return [memberRef.name || path[path.length - 1], memberDefinition];
    });
  }

  /**
   * This method is mainly used for evaluating RLS conditions and filters.
   * It allows referencing security_context (lowercase) in dynamic conditions or filter values.
   *
   * It currently does not support async calls because inner resolveSymbol and
   * resolveSymbolsCall are sync. Async support may be added later with deeper
   * refactoring.
   */
  protected evaluateContextFunction(cube: any, contextFn: any, context: any = {}) {
    const cubeEvaluator = this;

    return cubeEvaluator.resolveSymbolsCall(contextFn, (name: string) => {
      const resolvedSymbol = this.resolveSymbol(cube, name);
      if (resolvedSymbol) {
        return resolvedSymbol;
      }
      throw new UserError(
        `Cube references are not allowed when evaluating RLS conditions or filters. Found: ${name} in ${cube.name}`
      );
    }, {
      contextSymbols: {
        securityContext: context.securityContext,
      }
    });
  }

  protected evaluateReferences<T extends ToString | Array<ToString>>(
    cube: string | null,
    referencesFn: (...args: Array<unknown>) => T,
    options: { collectJoinHints?: boolean, originalSorting?: boolean } = {}
  ):
  T extends Array<ToString> ? Array<string> : T extends ToString ? string : string | Array<string> {
    const cubeEvaluator = this;

    const fullPath = (joinHints, path) => {
      if (joinHints?.length > 0) {
        return R.uniq(joinHints.concat(path));
      } else {
        return path;
      }
    };

    const arrayOrSingle: T = cubeEvaluator.resolveSymbolsCall(referencesFn, (name) => {
      const referencedCube = cubeEvaluator.symbols[name] && name || cube;
      const resolvedSymbol =
        cubeEvaluator.resolveSymbol(
          cube,
          name
        );
      // eslint-disable-next-line no-underscore-dangle
      // if (resolvedSymbol && resolvedSymbol._objectWithResolvedProperties) {
      if (resolvedSymbol._objectWithResolvedProperties) {
        return resolvedSymbol;
      }
      return cubeEvaluator.pathFromArray(fullPath(cubeEvaluator.joinHints(), [referencedCube, name]));
    }, {
      // eslint-disable-next-line no-shadow
      sqlResolveFn: (symbol, currentCube, refProperty, propertyName) => cubeEvaluator.pathFromArray(fullPath(cubeEvaluator.joinHints(), [currentCube, refProperty, ...(propertyName ? [propertyName] : [])])),
      // eslint-disable-next-line no-shadow
      cubeAliasFn: (currentCube) => cubeEvaluator.pathFromArray(fullPath(cubeEvaluator.joinHints(), [currentCube])),
      collectJoinHints: options.collectJoinHints,
    });
    if (!Array.isArray(arrayOrSingle)) {
      // arrayOrSingle is of type `T`, and we just checked that it is not an array
      // Which means it `T` be an object with `toString`, and result must be `string`
      // For any branch of return type that can can contain just an object it's OK to return string
      return arrayOrSingle.toString() as any;
    }

    const references: Array<string> = arrayOrSingle.map(p => p.toString());
    // arrayOrSingle is of type `T`, and we just checked that it is an array
    // Which means that both `T` and result must be arrays
    // For any branch of return type that can contain array it's OK to return array
    return options.originalSorting ? references : R.sortBy(R.identity, references) as any;
  }

  public pathFromArray(array: string[]): string {
    return array.join('.');
  }

  /**
   * Split join path to member to join hint and member path: `A.B.C.D.E.dim` => `[A, B, C, D, E]` + `E.dim`
   */
  public static joinHintFromPath(path: string): { path: string, joinHint: string[] } {
    const parts = path.split('.');
    if (parts.length > 2) {
      // Path contains join path
      const joinHint = parts.slice(0, -1);
      return {
        path: parts.slice(-2).join('.'),
        joinHint,
      };
    } else {
      return {
        path,
        joinHint: [],
      };
    }
  }

  protected resolveSymbolsCall<T>(
    func: (...args: Array<unknown>) => T | DynamicReference<T>,
    nameResolver: (id: string) => unknown,
    context?: unknown,
  ): T {
    const oldContext = this.resolveSymbolsCallContext;
    this.resolveSymbolsCallContext = context;
    try {
      // eslint-disable-next-line prefer-spread
      const res = func.apply(null, this.funcArguments(func).map((id) => nameResolver(id.trim())));
      if (res instanceof DynamicReference) {
        return res.fn.apply(null, res.memberNames.map((id) => nameResolver(id.trim())));
      }
      return res;
    } finally {
      this.resolveSymbolsCallContext = oldContext;
    }
  }

  protected withSymbolsCallContext(func: Function, context) {
    const oldContext = this.resolveSymbolsCallContext;
    this.resolveSymbolsCallContext = context;
    try {
      return func();
    } finally {
      this.resolveSymbolsCallContext = oldContext;
    }
  }

  public funcArguments(func: Function): string[] {
    const funcDefinition = func.toString();
    if (!this.funcArgumentsValues[funcDefinition]) {
      const match = funcDefinition.match(FunctionRegex);
      if (match && (match[1] || match[2] || match[3])) {
        this.funcArgumentsValues[funcDefinition] = (match[1] || match[2] || match[3]).split(',').map(s => s.trim());
      } else if (match) {
        this.funcArgumentsValues[funcDefinition] = [];
      } else {
        throw new Error(`Can't match args for: ${func.toString()}`);
      }
    }
    return this.funcArgumentsValues[funcDefinition];
  }

  protected joinHints(): string | string[] | undefined {
    const { joinHints } = this.resolveSymbolsCallContext || {};
    if (Array.isArray(joinHints)) {
      return R.uniq(joinHints);
    }
    return joinHints;
  }

  protected resolveSymbolsCallDeps(cubeName, sql) {
    try {
      const deps: any[] = [];
      this.resolveSymbolsCall(sql, (name) => {
        deps.push({ name });
        const resolvedSymbol = this.resolveSymbol(
          cubeName,
          name
        );
        if (resolvedSymbol._objectWithResolvedProperties) {
          return resolvedSymbol;
        }
        return '';
      }, {
        depsResolveFn: (name, parent) => {
          deps.push({ name, parent });
          return deps.length - 1;
        },
        currResolveIndexFn: () => deps.length - 1,
        contextSymbols: this.depsContextSymbols(),

      });
      return deps;
    } catch (e) {
      return [];
    }
  }

  protected depsContextSymbols() {
    return {
      filterParams: this.filtersProxyDep(),
      filterGroup: this.filterGroupFunctionDep(),
      securityContext: CubeSymbols.contextSymbolsProxyFrom({}, (param) => param),
      sqlUtils: {
        convertTz: (f) => f
      },
    };
  }

  protected filtersProxyDep() {
    return new Proxy({}, {
      get: (target, name) => {
        if (name === '_objectWithResolvedProperties') {
          return true;
        }
        // allFilters is null whenever it's used to test if the member is owned by cube so it should always render to `1 = 1`
        // and do not check cube validity as it's part of compilation step.
        // @ts-ignore
        const cubeName = this.cubeNameFromPath(name);
        return new Proxy({ cube: cubeName }, {
          get: (cubeNameObj, propertyName) => ({
            filter: (column) => ({
              __column() {
                return column;
              },
              __member() {
                // @ts-ignore
                return this.pathFromArray([cubeNameObj.cube, propertyName]);
              },
              toString() {
                return '';
              }
            })
          })
        });
      }
    });
  }

  protected filterGroupFunctionDep() {
    return (...filterParamArgs) => '';
  }

  public resolveSymbol(cubeName, name: string) {
    const { sqlResolveFn, contextSymbols, collectJoinHints, depsResolveFn, currResolveIndexFn } = this.resolveSymbolsCallContext || {};
    if (name === 'USER_CONTEXT') {
      throw new Error('Support for USER_CONTEXT was removed, please migrate to SECURITY_CONTEXT.');
    }

    if (CONTEXT_SYMBOLS[name]) {
      // always resolves if contextSymbols aren't passed for transpile step
      const symbol = contextSymbols?.[CONTEXT_SYMBOLS[name]] || {};
      // eslint-disable-next-line no-underscore-dangle
      symbol._objectWithResolvedProperties = true;
      return symbol;
    }

    // In proxied subProperty flow `name` will be set to parent dimension|measure name,
    // so there will be no cube = this.symbols[cubeName : name] found, but potentially
    // during cube definition evaluation some other deeper subProperty may be requested.
    // To distinguish such cases we pass the right now requested property name to
    // cubeReferenceProxy, so later if subProperty is requested we'll have all the required
    // information to construct the response.
    let cube = this.symbols[this.isCurrentCube(name) ? cubeName : name];
    if (sqlResolveFn) {
      if (cube) {
        cube = this.cubeReferenceProxy(
          this.isCurrentCube(name) ? cubeName : name,
          collectJoinHints ? [] : undefined
        );
      } else if (this.symbols[cubeName]?.[name]) {
        cube = this.cubeReferenceProxy(
          cubeName,
          collectJoinHints ? [] : undefined,
          name
        );
      }
    } else if (depsResolveFn) {
      if (cube) {
        const newCubeName = this.isCurrentCube(name) ? cubeName : name;
        const parentIndex = currResolveIndexFn();
        cube = this.cubeDependenciesProxy(parentIndex, newCubeName);
        return cube;
      } else if (this.symbols[cubeName]?.[name] && this.symbols[cubeName][name].type === 'time') {
        const parentIndex = currResolveIndexFn();
        return this.timeDimDependenciesProxy(parentIndex);
      }
    }
    return cube || this.symbols[cubeName]?.[name];
  }

  protected cubeReferenceProxy(cubeName, joinHints?: any[], refProperty?: any) {
    if (joinHints) {
      joinHints = joinHints.concat(cubeName);
    }
    const self = this;
    const { sqlResolveFn, cubeAliasFn, query, cubeReferencesUsed } = self.resolveSymbolsCallContext || {};
    return new Proxy({}, {
      get: (v, propertyName) => {
        if (propertyName === '_objectWithResolvedProperties') {
          return true;
        }
        if (propertyName === '__cubeName') {
          return cubeName;
        }
        const cube = self.symbols[cubeName];
        // first phase of compilation
        if (!cube) {
          if (propertyName === 'toString') {
            return cubeName;
          }
          return undefined;
        }
        if (propertyName === 'toString') {
          if (refProperty) {
            return () => this.withSymbolsCallContext(
              () => sqlResolveFn(cube[refProperty], cubeName, refProperty),
              { ...this.resolveSymbolsCallContext, joinHints }
            );
          }

          return () => {
            if (query) {
              query.pushCubeNameForCollectionIfNecessary(cube.cubeName());
              query.pushJoinHints(joinHints);
            }
            if (cubeReferencesUsed) {
              cubeReferencesUsed.push(cube.cubeName());
            }
            return cubeAliasFn && this.withSymbolsCallContext(
              () => cubeAliasFn(cube.cubeName()),
              { ...this.resolveSymbolsCallContext, joinHints }
            ) || cube.cubeName();
          };
        }
        if (propertyName === 'sql') {
          return () => query.cubeSql(cube.cubeName());
        }
        if (refProperty &&
          cube[refProperty].type === 'time' &&
          self.resolveGranularity([cubeName, refProperty, 'granularities', propertyName], cube)
        ) {
          return {
            toString: () => this.withSymbolsCallContext(
              () => sqlResolveFn(cube[refProperty], cubeName, refProperty, propertyName),
              { ...this.resolveSymbolsCallContext },
            ),
          };
        }
        if (cube[propertyName as string]) {
          // We put cubeName at the beginning of the cubeReferenceProxy(), no need to add it again
          // so let's cut it off from joinHints
          return this.cubeReferenceProxy(cubeName, joinHints?.slice(0, -1), propertyName);
        }
        if (self.symbols[propertyName]) {
          return this.cubeReferenceProxy(propertyName, joinHints);
        }
        if (typeof propertyName === 'string') {
          throw new UserError(`${cubeName}${refProperty ? `.${refProperty}` : ''}.${propertyName} cannot be resolved. There's no such member or cube.`);
        }
        return undefined;
      }
    });
  }

  /**
   * Tries to resolve Granularity object.
   * For predefined granularity it constructs it on the fly.
   * @param {string|string[]} path
   * @param [refCube] Optional cube object to operate on
   */
  public resolveGranularity(path: string | string[], refCube?: any) {
    const [cubeName, dimName, gr, granName] = Array.isArray(path) ? path : path.split('.');
    const cube = refCube || this.symbols[cubeName];

    // Calendar cubes time dimensions may define custom sql for predefined granularities,
    // so we need to check if such granularity exists in cube definition.
    if (typeof granName === 'string' && /^(second|minute|hour|day|week|month|quarter|year)$/i.test(granName)) {
      const customGranularity = cube?.[dimName]?.[gr]?.[granName];
      if (customGranularity) {
        return {
          ...customGranularity,
          interval: `1 ${granName}`, // It's still important to have interval for granularity math
        };
      }

      return { interval: `1 ${granName}` };
    }

    return cube?.[dimName]?.[gr]?.[granName];
  }

  protected cubeDependenciesProxy(parentIndex, cubeName) {
    const self = this;
    const { depsResolveFn } = self.resolveSymbolsCallContext || {};
    return new Proxy({}, {
      get: (v, propertyName) => {
        if (propertyName === '__cubeName') {
          depsResolveFn('__cubeName', parentIndex);
          return cubeName;
        }
        const cube = self.symbols[cubeName];

        if (propertyName === 'toString') {
          depsResolveFn('toString', parentIndex);
          return () => '';
        }
        if (propertyName === 'sql') {
          depsResolveFn('sql', parentIndex);
          return () => '';
        }
        if (propertyName === '_objectWithResolvedProperties') {
          return true;
        }
        if (cube[propertyName as string]) {
          const index = depsResolveFn(propertyName, parentIndex);
          if (cube[propertyName as string].type === 'time') {
            return this.timeDimDependenciesProxy(index);
          }

          return '';
        }
        if (self.symbols[propertyName]) {
          const index = depsResolveFn(propertyName, parentIndex);
          return this.cubeDependenciesProxy(index, propertyName);
        }
        if (typeof propertyName === 'string') {
          throw new UserError(`${cubeName}.${propertyName} cannot be resolved. There's no such member or cube.`);
        }
        return undefined;
      }
    });
  }

  protected timeDimDependenciesProxy(parentIndex) {
    const self = this;
    const { depsResolveFn } = self.resolveSymbolsCallContext || {};
    return new Proxy({}, {
      get: (v, propertyName) => {
        if (propertyName === '_objectWithResolvedProperties') {
          return true;
        }
        if (propertyName === 'toString') {
          return () => '';
        }
        if (typeof propertyName === 'string') {
          depsResolveFn(propertyName, parentIndex);
        }
        return undefined;
      }
    });
  }

  public isCurrentCube(name) {
    return CURRENT_CUBE_CONSTANTS.indexOf(name) >= 0;
  }

  public static isCalculatedMeasureType(type: string): boolean {
    return type === 'number' || type === 'string' || type === 'time' || type === 'boolean';
  }

  /**
   TODO: support type qualifiers on min and max
   */
  public static toMemberDataType(type: string): string {
    return this.isCalculatedMeasureType(type) ? type : 'number';
  }

  public static contextSymbolsProxyFrom(symbols: object, allocateParam: (param: unknown) => unknown): object {
    return new Proxy(symbols, {
      get: (target, name) => {
        const propValue = target[name];
        const methods = (paramValue) => ({
          filter: (column) => {
            if (paramValue) {
              const value = Array.isArray(paramValue) ?
                paramValue.map(allocateParam) :
                allocateParam(paramValue);
              if (typeof column === 'function') {
                return column(value);
              } else {
                return `${column} = ${value}`;
              }
            } else {
              return '1 = 1';
            }
          },
          requiredFilter: (column) => {
            if (!paramValue) {
              throw new UserError(`Filter for ${column} is required`);
            }
            return methods(paramValue).filter(column);
          },
          unsafeValue: () => paramValue
        });
        return methods(target)[name] ||
          typeof propValue === 'object' && propValue !== null && CubeSymbols.contextSymbolsProxyFrom(propValue, allocateParam) ||
          methods(propValue);
      }
    });
  }
}
