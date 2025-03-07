import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';
import { camelize } from 'inflection';

import { UserError } from './UserError';
import { DynamicReference } from './DynamicReference';
import { camelizeCube } from './utils';
import { BaseQuery } from '../adapter';

import type { ErrorReporter } from './ErrorReporter';

interface CubeDefinition {
  name: string;
  extends?: string;
  measures?: Record<string, any>;
  dimensions?: Record<string, any>;
  segments?: Record<string, any>;
  hierarchies?: Record<string, any>;
  preAggregations?: Record<string, any>;
  joins?: Record<string, any>;
  accessPolicy?: Record<string, any>;
  includes?: any;
  excludes?: any;
  cubes?: any;
  isView?: boolean;
  isSplitView?: boolean;
  includedMembers?: any[];
}

interface SplitViews {
  [key: string]: any;
}

const FunctionRegex = /function\s+\w+\(([A-Za-z0-9_,]*)|\(([\s\S]*?)\)\s*=>|\(?(\w+)\)?\s*=>/;
export const CONTEXT_SYMBOLS = {
  SECURITY_CONTEXT: 'securityContext',
  // SECURITY_CONTEXT has been deprecated, however security_context (lowecase)
  // is allowed in RBAC policies for query-time attribute matching
  security_context: 'securityContext',
  securityContext: 'securityContext',
  FILTER_PARAMS: 'filterParams',
  FILTER_GROUP: 'filterGroup',
  SQL_UTILS: 'sqlUtils'
};

export const CURRENT_CUBE_CONSTANTS = ['CUBE', 'TABLE'];

export class CubeSymbols {
  public symbols: Record<string | symbol, any>;

  private builtCubes: Record<string, any>;

  private cubeDefinitions: Record<string, CubeDefinition>;

  private funcArgumentsValues: Record<string, string[]>;

  public cubeList: any[];

  private evaluateViews: boolean;

  private resolveSymbolsCallContext: any;

  public constructor(evaluateViews = false) {
    this.symbols = {};
    this.builtCubes = {};
    this.cubeDefinitions = {};
    this.funcArgumentsValues = {};
    this.cubeList = [];
    this.evaluateViews = evaluateViews;
  }

  public compile(cubes: CubeDefinition[], errorReporter: ErrorReporter) {
    // @ts-ignore
    this.cubeDefinitions = R.pipe(
      // @ts-ignore
      R.map((c: CubeDefinition) => [c.name, c]),
      R.fromPairs
      // @ts-ignore
    )(cubes);
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
      }
    }
  }

  public getCubeDefinition(cubeName: string) {
    if (!this.builtCubes[cubeName]) {
      const cubeDefinition = this.cubeDefinitions[cubeName];
      this.builtCubes[cubeName] = this.createCube(cubeDefinition);
    }

    return this.builtCubes[cubeName];
  }

  public createCube(cubeDefinition: CubeDefinition) {
    let measures: any;
    let dimensions: any;
    let segments: any;
    let hierarchies: any;

    const cubeObject = Object.assign({
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
      get measures() {
        if (!measures) {
          measures = this.allDefinitions('measures');
        }
        return measures;
      },
      set measures(v) {
        // Dont allow to modify
      },

      get dimensions() {
        if (!dimensions) {
          dimensions = this.allDefinitions('dimensions');
        }
        return dimensions;
      },
      set dimensions(v) {
        // Dont allow to modify
      },

      get segments() {
        if (!segments) {
          segments = this.allDefinitions('segments');
        }
        return segments;
      },
      set segments(v) {
        // Dont allow to modify
      },

      get hierarchies() {
        if (!hierarchies) {
          hierarchies = this.allDefinitions('hierarchies');
        }
        return hierarchies;
      },
      set hierarchies(v) {
        //
      }
    },
    cubeDefinition);

    if (cubeDefinition.extends) {
      const superCube = this.resolveSymbolsCall(cubeDefinition.extends, (name: string) => this.cubeReferenceProxy(name));
      Object.setPrototypeOf(
        cubeObject,
        // eslint-disable-next-line no-underscore-dangle
        superCube.__cubeName ? this.getCubeDefinition(superCube.__cubeName) : superCube
      );
    }

    return cubeObject;
  }

  protected transform(cubeName: string, errorReporter: ErrorReporter, splitViews: SplitViews) {
    const cube = this.getCubeDefinition(cubeName);
    const duplicateNames = R.compose(
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

    // @ts-ignore
    if (duplicateNames.length > 0) {
      // @ts-ignore
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

    return Object.assign(
      { cubeName: () => cube.name, cubeObj: () => cube },
      cube.measures || {},
      cube.dimensions || {},
      cube.segments || {},
      cube.preAggregations || {}
    );
  }

  private camelCaseTypes(obj: Object) {
    if (!obj) {
      return;
    }

    // eslint-disable-next-line no-restricted-syntax
    for (const member of Object.values(obj)) {
      if (member.type && member.type.indexOf('_') !== -1) {
        member.type = camelize(member.type, true);
      }
      if (member.relationship && member.relationship.indexOf('_') !== -1) {
        member.relationship = camelize(member.relationship, true);
      }
    }
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

  protected prepareIncludes(cube: CubeDefinition, errorReporter: ErrorReporter, splitViews: SplitViews) {
    if (!cube.includes && !cube.cubes) {
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
      if (cube.cubes) {
        // If the hierarchy is included all members from it should be included as well
        // Extend `includes` with members from hierarchies that should be auto-included
        const cubes = type === 'dimensions' ? cube.cubes.map((it) => {
          const fullPath = this.evaluateReferences(null, it.joinPath, { collectJoinHints: true });
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
        }) : cube.cubes;

        cubeIncludes = this.membersFromCubes(cube, cubes, type, errorReporter, splitViews, memberSets) || [];
      }

      // This is the deprecated approach
      const includes = cube.includes && this.membersFromIncludeExclude(cube.includes, cube.name, type) || [];
      const excludes = cube.excludes && this.membersFromIncludeExclude(cube.excludes, cube.name, type) || [];

      // cube includes will take precedence in case of member clash
      const finalIncludes = this.diffByMember(
        this.diffByMember(includes, cubeIncludes).concat(cubeIncludes),
        excludes
      );

      if (type === 'hierarchies') {
        for (const member of finalIncludes) {
          const path = member.member.split('.');
          const cubeName = path[path.length - 2];
          const hierarchyName = path[path.length - 1];
          const hierarchy = this.getResolvedMember(type, cubeName, hierarchyName);

          if (hierarchy) {
            const levels = this.evaluateReferences(cubeName, this.getResolvedMember('hierarchies', cubeName, hierarchyName).levels, { originalSorting: true });

            levels.forEach((level) => autoIncludeMembers.add(level));
          }
        }
      }

      const includeMembers = this.generateIncludeMembers(finalIncludes, cube.name, type);
      this.applyIncludeMembers(includeMembers, cube, type, errorReporter);

      cube.includedMembers = [...(cube.includedMembers || []), ...Array.from(new Set(finalIncludes.map((it: any) => {
        const split = it.member.split('.');
        const memberPath = this.pathFromArray([split[split.length - 2], split[split.length - 1]]);
        return {
          type,
          memberPath,
          name: it.name
        };
      })))];
    }

    [...memberSets.allMembers].filter(it => !memberSets.resolvedMembers.has(it)).forEach(it => {
      errorReporter.error(`Member '${it}' is included in '${cube.name}' but not defined in any cube`);
    });
  }

  protected applyIncludeMembers(includeMembers: any[], cube: CubeDefinition, type: string, errorReporter: ErrorReporter) {
    for (const [memberName, memberDefinition] of includeMembers) {
      if (cube[type]?.[memberName]) {
        errorReporter.error(`Included member '${memberName}' conflicts with existing member of '${cube.name}'. Please consider excluding this member or assigning it an alias.`);
      } else if (type !== 'hierarchies') {
        cube[type][memberName] = memberDefinition;
      }
    }
  }

  protected membersFromCubes(parentCube: CubeDefinition, cubes: any[], type: string, errorReporter: ErrorReporter, splitViews: SplitViews, memberSets: any) {
    return R.unnest(cubes.map(cubeInclude => {
      const fullPath = this.evaluateReferences(null, cubeInclude.joinPath, { collectJoinHints: true });
      const split = fullPath.split('.');
      const cubeReference = split[split.length - 1];
      const cubeName = cubeInclude.alias || cubeReference;

      let includes;
      const fullMemberName = (memberName: string) => (cubeInclude.prefix ? `${cubeName}_${memberName}` : memberName);

      if (cubeInclude.includes === '*') {
        const membersObj = this.symbols[cubeReference]?.cubeObj()?.[type] || {};
        includes = Object.keys(membersObj).map(memberName => ({ member: `${fullPath}.${memberName}`, name: fullMemberName(memberName) }));
      } else {
        includes = cubeInclude.includes.map((include: any) => {
          const member = include.alias || include;

          if (member.includes('.')) {
            errorReporter.error(`Paths aren't allowed in cube includes but '${member}' provided as include member`);
          }

          const name = fullMemberName(include.alias || member);
          memberSets.allMembers.add(name);

          const includedMemberName = include.name || include;

          const resolvedMember = this.getResolvedMember(type, cubeReference, includedMemberName) ? {
            member: `${fullPath}.${includedMemberName}`,
            name,
          } : undefined;

          if (resolvedMember) {
            memberSets.resolvedMembers.add(name);
          }

          return resolvedMember;
        });
      }

      const excludes = (cubeInclude.excludes || []).map((exclude: any) => {
        if (exclude.includes('.')) {
          errorReporter.error(`Paths aren't allowed in cube excludes but '${exclude}' provided as exclude member`);
        }

        const resolvedMember = this.getResolvedMember(type, cubeReference, exclude);
        return resolvedMember ? {
          member: `${fullPath}.${exclude}`
        } : undefined;
      });

      const finalIncludes = this.diffByMember(includes.filter(Boolean), excludes.filter(Boolean));

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

        const includeMembers = this.generateIncludeMembers(finalIncludes, parentCube.name, type);
        this.applyIncludeMembers(includeMembers, splitViewDef, type, errorReporter);

        return [];
      } else {
        return finalIncludes;
      }
    }));
  }

  protected diffByMember(includes: any[], excludes: any[]) {
    const excludesMap = new Map();

    for (const exclude of excludes) {
      excludesMap.set(exclude.member, true);
    }

    return includes.filter(include => !excludesMap.has(include.member));
  }

  protected membersFromIncludeExclude(referencesFn: any, cubeName: string, type: string) {
    const references = this.evaluateReferences(cubeName, referencesFn);
    return R.unnest(references.map((ref: string) => {
      const path = ref.split('.');
      if (path.length === 1) {
        const membersObj = this.symbols[path[0]]?.cubeObj()?.[type] || {};
        return Object.keys(membersObj).map(memberName => ({ member: `${ref}.${memberName}` }));
      } else if (path.length === 2) {
        const resolvedMember = this.getResolvedMember(type, path[0], path[1]);
        return resolvedMember ? [{ member: ref }] : undefined;
      } else {
        throw new Error(`Unexpected path length ${path.length} for ${ref}`);
      }
    })).filter(Boolean);
  }

  protected getResolvedMember(type: string, cubeName: string, memberName: string) {
    return this.symbols[cubeName]?.cubeObj()?.[type]?.[memberName];
  }

  protected generateIncludeMembers(members: any[], cubeName: string, type: string) {
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
          type: BaseQuery.toMemberDataType(resolvedMember.type),
          aggType: resolvedMember.type,
          meta: resolvedMember.meta,
          title: resolvedMember.title,
          description: resolvedMember.description,
          format: resolvedMember.format,
        };
      } else if (type === 'dimensions') {
        memberDefinition = {
          ...(resolvedMember.granularities ? { granularities: resolvedMember.granularities } : {}),
          sql,
          type: resolvedMember.type,
          meta: resolvedMember.meta,
          title: resolvedMember.title,
          description: resolvedMember.description,
          format: resolvedMember.format,
        };
      } else if (type === 'segments') {
        memberDefinition = {
          sql,
          meta: resolvedMember.meta,
          description: resolvedMember.description,
        };
      } else if (type === 'hierarchies') {
        memberDefinition = {
          title: resolvedMember.title,
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
   * It allows referencing security_context (lowecase) in dynamic conditions or filter values.
   *
   * It currently does not support async calls because inner resolveSymbol and
   * resolveSymbolsCall are sync. Async support may be added later with deeper
   * refactoring.
   */
  protected evaluateContextFunction(cube: any, contextFn: any, context: any = {}) {
    const cubeEvaluator = this;

    const res = cubeEvaluator.resolveSymbolsCall(contextFn, (name: string) => {
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

    return res;
  }

  protected evaluateReferences(cube, referencesFn, options: any = {}) {
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
      // if (resolvedSymbol && resolvedSymbol._objectWithResolvedProperties) {
      if (resolvedSymbol._objectWithResolvedProperties) {
        return resolvedSymbol;
      }
      return cubeEvaluator.pathFromArray(fullPath(cubeEvaluator.joinHints(), [referencedCube, name]));
    }, {
      // eslint-disable-next-line no-shadow
      sqlResolveFn: (symbol, currentCube, n) => cubeEvaluator.pathFromArray(fullPath(cubeEvaluator.joinHints(), [currentCube, n])),
      // eslint-disable-next-line no-shadow
      cubeAliasFn: (currentCube) => cubeEvaluator.pathFromArray(fullPath(cubeEvaluator.joinHints(), [currentCube])),
      collectJoinHints: options.collectJoinHints,
    });
    if (!Array.isArray(arrayOrSingle)) {
      return arrayOrSingle.toString();
    }

    const references = arrayOrSingle.map(p => p.toString());
    return options.originalSorting ? references : R.sortBy(R.identity, references);
  }

  public pathFromArray(array) {
    return array.join('.');
  }

  protected resolveSymbolsCall(func, nameResolver, context?: any) {
    const oldContext = this.resolveSymbolsCallContext;
    this.resolveSymbolsCallContext = context;
    try {
      // eslint-disable-next-line prefer-spread
      let res = func.apply(null, this.funcArguments(func).map((id) => nameResolver(id.trim())));
      if (res instanceof DynamicReference) {
        res = res.fn.apply(null, res.memberNames.map((id) => nameResolver(id.trim())));
      }
      return res;
    } finally {
      this.resolveSymbolsCallContext = oldContext;
    }
  }

  protected withSymbolsCallContext(func, context) {
    const oldContext = this.resolveSymbolsCallContext;
    this.resolveSymbolsCallContext = context;
    try {
      return func();
    } finally {
      this.resolveSymbolsCallContext = oldContext;
    }
  }

  protected funcArguments(func) {
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

  protected joinHints() {
    const { joinHints } = this.resolveSymbolsCallContext || {};
    return joinHints;
  }

  protected resolveSymbolsCallDeps(cubeName, sql) {
    try {
      return this.resolveSymbolsCallDeps2(cubeName, sql);
    } catch (e) {
      console.log(e);
      return [];
    }
  }

  protected resolveSymbolsCallDeps2(cubeName, sql) {
    const deps: any[] = [];
    this.resolveSymbolsCall(sql, (name) => {
      deps.push({ name, undefined });
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
  }

  protected depsContextSymbols() {
    return Object.assign({
      filterParams: this.filtersProxyDep(),
      filterGroup: this.filterGroupFunctionDep(),
      securityContext: BaseQuery.contextSymbolsProxyFrom({}, (param) => param),
      sqlUtils: {
        convertTz: (f) => f

      },
    });
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

  public resolveSymbol(cubeName, name) {
    const { sqlResolveFn, contextSymbols, collectJoinHints, depsResolveFn, currResolveIndexFn } = this.resolveSymbolsCallContext || {};

    if (name === 'USER_CONTEXT') {
      throw new Error('Support for USER_CONTEXT was removed, please migrate to SECURITY_CONTEXT.');
    }

    if (CONTEXT_SYMBOLS[name]) {
      // always resolves if contextSymbols aren't passed for transpile step
      const symbol = contextSymbols && contextSymbols[CONTEXT_SYMBOLS[name]] || {};
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
          undefined,
          name
        );
      }
    } else if (depsResolveFn) {
      if (cube) {
        const newCubeName = this.isCurrentCube(name) ? cubeName : name;
        const parentIndex = currResolveIndexFn();
        cube = this.cubeDependenciesProxy(parentIndex, newCubeName);
        return cube;
      }
    }
    return cube || (this.symbols[cubeName] && this.symbols[cubeName][name]);
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
        if (cube[propertyName]) {
          return this.cubeReferenceProxy(cubeName, joinHints, propertyName);
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

    // Predefined granularity
    if (typeof granName === 'string' && /^(second|minute|hour|day|week|month|quarter|year)$/i.test(granName)) {
      return { interval: `1 ${granName}` };
    }

    return cube && cube[dimName] && cube[dimName][gr] && cube[dimName][gr][granName];
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
        if (cube[propertyName]) {
          depsResolveFn(propertyName, parentIndex);
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

  public isCurrentCube(name) {
    return CURRENT_CUBE_CONSTANTS.indexOf(name) >= 0;
  }
}
