import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';
import { camelize } from 'inflection';

import { UserError } from './UserError';
import { DynamicReference } from './DynamicReference';
import { camelizeCube } from './utils';
import { BaseQuery } from '../adapter';

const FunctionRegex = /function\s+\w+\(([A-Za-z0-9_,]*)|\(([\s\S]*?)\)\s*=>|\(?(\w+)\)?\s*=>/;
const CONTEXT_SYMBOLS = {
  SECURITY_CONTEXT: 'securityContext',
  // SECURITY_CONTEXT has been deprecated, however security_context (lowecase)
  // is allowed in RBAC policies for query-time attribute matching
  security_context: 'securityContext',
  securityContext: 'securityContext',
  FILTER_PARAMS: 'filterParams',
  FILTER_GROUP: 'filterGroup',
  SQL_UTILS: 'sqlUtils'
};

const CURRENT_CUBE_CONSTANTS = ['CUBE', 'TABLE'];

export class CubeSymbols {
  constructor(evaluateViews) {
    this.symbols = {};
    this.builtCubes = {};
    this.cubeDefinitions = {};
    this.funcArgumentsValues = {};
    this.cubeList = [];
    this.evaluateViews = evaluateViews || false;
  }

  compile(cubes, errorReporter) {
    this.cubeDefinitions = R.pipe(
      R.map(c => [c.name, c]),
      R.fromPairs
    )(cubes);
    this.cubeList = cubes.map(c => (c.name ? this.getCubeDefinition(c.name) : this.createCube(c)));
    // TODO support actual dependency sorting to allow using views inside views
    const sortedByDependency = R.pipe(
      R.sortBy(c => !!c.isView),
    )(cubes);
    for (const cube of sortedByDependency) {
      const splitViews = {};
      this.symbols[cube.name] = this.transform(cube.name, errorReporter.inContext(`${cube.name} cube`), splitViews);
      for (const viewName of Object.keys(splitViews)) {
        // TODO can we define it when cubeList is defined?
        this.cubeList.push(splitViews[viewName]);
        this.symbols[viewName] = splitViews[viewName];
      }
    }
  }

  getCubeDefinition(cubeName) {
    if (!this.builtCubes[cubeName]) {
      const cubeDefinition = this.cubeDefinitions[cubeName];
      this.builtCubes[cubeName] = this.createCube(cubeDefinition);
    }

    return this.builtCubes[cubeName];
  }

  createCube(cubeDefinition) {
    let measures;
    let dimensions;
    let segments;
    let hierarchies;

    const cubeObject = Object.assign({
      allDefinitions(type) {
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
      const superCube = this.resolveSymbolsCall(cubeDefinition.extends, (name) => this.cubeReferenceProxy(name));
      Object.setPrototypeOf(
        cubeObject,
        // eslint-disable-next-line no-underscore-dangle
        superCube.__cubeName ? this.getCubeDefinition(superCube.__cubeName) : superCube
      );
    }

    return cubeObject;
  }

  transform(cubeName, errorReporter, splitViews) {
    const cube = this.getCubeDefinition(cubeName);
    const duplicateNames = R.compose(
      R.map(nameToDefinitions => nameToDefinitions[0]),
      R.toPairs,
      R.filter(definitionsByName => definitionsByName.length > 1),
      R.groupBy(nameToDefinition => nameToDefinition[0]),
      R.unnest,
      R.map(R.toPairs),
      R.filter(v => !!v)
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

    return Object.assign(
      { cubeName: () => cube.name, cubeObj: () => cube },
      cube.measures || {},
      cube.dimensions || {},
      cube.segments || {},
      cube.preAggregations || {}
    );
  }

  /**
   * @private
   */
  camelCaseTypes(obj) {
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

  transformPreAggregations(preAggregations) {
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

  transformPreAggregationIndexes(indexes) {
    for (const index of Object.values(indexes)) {
      if (!index.type) {
        index.type = 'regular';
      }
    }
  }

  /**
   * @protected
   */
  prepareIncludes(cube, errorReporter, splitViews) {
    if (!cube.includes && !cube.cubes) {
      return;
    }

    const memberSets = {
      resolvedMembers: new Set(),
      allMembers: new Set(),
    };

    const types = ['measures', 'dimensions', 'segments', 'hierarchies'];
    for (const type of types) {
      const cubeIncludes = cube.cubes && this.membersFromCubes(cube, cube.cubes, type, errorReporter, splitViews, memberSets) || [];

      const includes = cube.includes && this.membersFromIncludeExclude(cube.includes, cube.name, type) || [];
      const excludes = cube.excludes && this.membersFromIncludeExclude(cube.excludes, cube.name, type) || [];

      // cube includes will take precedence in case of member clash
      const finalIncludes = this.diffByMember(
        this.diffByMember(includes, cubeIncludes).concat(cubeIncludes),
        excludes
      );

      const includeMembers = this.generateIncludeMembers(finalIncludes, cube.name, type);
      this.applyIncludeMembers(includeMembers, cube, type, errorReporter);

      cube.includedMembers = [...(cube.includedMembers || []), ...Array.from(new Set(finalIncludes.map((it) => {
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

  applyIncludeMembers(includeMembers, cube, type, errorReporter) {
    for (const [memberName, memberDefinition] of includeMembers) {
      if (cube[type]?.[memberName]) {
        errorReporter.error(`Included member '${memberName}' conflicts with existing member of '${cube.name}'. Please consider excluding this member.`);
      } else if (type !== 'hierarchies') {
        cube[type][memberName] = memberDefinition;
      }
    }
  }

  /**
   * @protected
   */
  membersFromCubes(parentCube, cubes, type, errorReporter, splitViews, memberSets) {
    return R.unnest(cubes.map(cubeInclude => {
      const fullPath = this.evaluateReferences(null, cubeInclude.joinPath, { collectJoinHints: true });
      const split = fullPath.split('.');
      const cubeReference = split[split.length - 1];
      const cubeName = cubeInclude.alias || cubeReference;

      let includes;
      const fullMemberName = (memberName) => (cubeInclude.prefix ? `${cubeName}_${memberName}` : memberName);

      if (cubeInclude.includes === '*') {
        const membersObj = this.symbols[cubeReference]?.cubeObj()?.[type] || {};
        if (Array.isArray(membersObj)) {
          includes = membersObj.map(it => ({ member: `${fullPath}.${it.name}`, name: fullMemberName(it.name) }));
        } else {
          includes = Object.keys(membersObj).map(memberName => ({ member: `${fullPath}.${memberName}`, name: fullMemberName(memberName) }));
        }
      } else {
        includes = cubeInclude.includes.map(include => {
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

      const excludes = (cubeInclude.excludes || []).map(exclude => {
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

  diffByMember(includes, excludes) {
    const excludesMap = new Map();

    for (const exclude of excludes) {
      excludesMap.set(exclude.member, true);
    }

    return includes.filter(include => !excludesMap.has(include.member));
  }

  membersFromIncludeExclude(referencesFn, cubeName, type) {
    const references = this.evaluateReferences(cubeName, referencesFn);
    return R.unnest(references.map(ref => {
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

  /**
   * @protected
   */
  getResolvedMember(type, cubeName, memberName) {
    if (Array.isArray(this.symbols[cubeName]?.cubeObj()?.[type])) {
      return this.symbols[cubeName]?.cubeObj()?.[type]?.find((it) => it.name === memberName);
    }

    return this.symbols[cubeName]?.cubeObj()?.[type]?.[memberName];
  }

  /**
   * @protected
   */
  generateIncludeMembers(members, cubeName, type) {
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
  evaluateContextFunction(cube, contextFn, context = {}) {
    const cubeEvaluator = this;

    const res = cubeEvaluator.resolveSymbolsCall(contextFn, (name) => {
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
      // if (resolvedSymbol && resolvedSymbol._objectWithResolvedProperties) {
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

  pathFromArray(array) {
    return array.join('.');
  }

  resolveSymbolsCall(func, nameResolver, context) {
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

  withSymbolsCallContext(func, context) {
    const oldContext = this.resolveSymbolsCallContext;
    this.resolveSymbolsCallContext = context;
    try {
      return func();
    } finally {
      this.resolveSymbolsCallContext = oldContext;
    }
  }

  funcArguments(func) {
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

  joinHints() {
    const { joinHints } = this.resolveSymbolsCallContext || {};
    return joinHints;
  }

  resolveSymbolsCallDeps(cubeName, sql) {
    try {
      return this.resolveSymbolsCallDeps2(cubeName, sql);
    } catch (e) {
      console.log(e);
      return [];
    }
  }

  resolveSymbolsCallDeps2(cubeName, sql) {
    const deps = [];
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

  depsContextSymbols() {
    return Object.assign({
      filterParams: this.filtersProxyDep(),
      filterGroup: this.filterGroupFunctionDep(),
      securityContext: BaseQuery.contextSymbolsProxyFrom({}, (param) => param)
    });
  }

  filtersProxyDep() {
    return new Proxy({}, {
      get: (target, name) => {
        if (name === '_objectWithResolvedProperties') {
          return true;
        }
        // allFilters is null whenever it's used to test if the member is owned by cube so it should always render to `1 = 1`
        // and do not check cube validity as it's part of compilation step.
        const cubeName = this.cubeNameFromPath(name);
        return new Proxy({ cube: cubeName }, {
          get: (cubeNameObj, propertyName) => ({
            filter: (column) => ({
              __column() {
                return column;
              },
              __member() {
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

  filterGroupFunctionDep() {
    return (...filterParamArgs) => '';
  }

  resolveSymbol(cubeName, name) {
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

  cubeReferenceProxy(cubeName, joinHints, refProperty) {
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
  resolveGranularity(path, refCube) {
    const [cubeName, dimName, gr, granName] = Array.isArray(path) ? path : path.split('.');
    const cube = refCube || this.symbols[cubeName];

    // Predefined granularity
    if (typeof granName === 'string' && /^(second|minute|hour|day|week|month|quarter|year)$/i.test(granName)) {
      return { interval: `1 ${granName}` };
    }

    return cube && cube[dimName] && cube[dimName][gr] && cube[dimName][gr][granName];
  }

  cubeDependenciesProxy(parentIndex, cubeName) {
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

  isCurrentCube(name) {
    return CURRENT_CUBE_CONSTANTS.indexOf(name) >= 0;
  }
}
