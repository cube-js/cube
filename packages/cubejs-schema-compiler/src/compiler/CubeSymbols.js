import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';
import { camelize } from 'inflection';

import { UserError } from './UserError';
import { DynamicReference } from './DynamicReference';
import { camelizeCube } from './utils';
import { BaseQuery } from '../adapter';

const FunctionRegex = /function\s+\w+\(([A-Za-z0-9_,]*)|\(([\s\S]*?)\)\s*=>|\(?(\w+)\)?\s*=>/;
const CONTEXT_SYMBOLS = {
  USER_CONTEXT: 'securityContext',
  SECURITY_CONTEXT: 'securityContext',
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

    const cubeObject = Object.assign({
      allDefinitions(type) {
        if (cubeDefinition.extends) {
          return {
            ...super.allDefinitions(type),
            ...cubeDefinition[type]
          };
        } else {
          // TODO We probably do not need this shallow copy
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
      }
    }, cubeDefinition);

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
    )([cube.measures, cube.dimensions, cube.segments, cube.preAggregations]);
    if (duplicateNames.length > 0) {
      errorReporter.error(`${duplicateNames.join(', ')} defined more than once`);
    }

    camelizeCube(cube);

    this.camelCaseTypes(cube.joins);
    this.camelCaseTypes(cube.measures);
    this.camelCaseTypes(cube.dimensions);
    this.camelCaseTypes(cube.segments);
    this.camelCaseTypes(cube.preAggregations);

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

    const types = ['measures', 'dimensions', 'segments'];
    for (const type of types) {
      const cubeIncludes = cube.cubes && this.membersFromCubes(cube, cube.cubes, type, errorReporter, splitViews) || [];
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
          memberPath,
          name: it.name
        };
      })))];
    }
  }

  applyIncludeMembers(includeMembers, cube, type, errorReporter) {
    for (const [memberName, memberDefinition] of includeMembers) {
      if (cube[type]?.[memberName]) {
        errorReporter.error(`Included member '${memberName}' conflicts with existing member of '${cube.name}'. Please consider excluding this member.`);
      } else {
        cube[type][memberName] = memberDefinition;
      }
    }
  }

  /**
   * @protected
   */
  membersFromCubes(parentCube, cubes, type, errorReporter, splitViews) {
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
          type: BaseQuery.toMemberDataType(resolvedMember.type),
          aggType: resolvedMember.type,
          meta: resolvedMember.meta,
          title: resolvedMember.title,
          description: resolvedMember.description,
          format: resolvedMember.format,
        };
      } else if (type === 'dimensions') {
        memberDefinition = {
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
      } else {
        throw new Error(`Unexpected member type: ${type}`);
      }
      return [memberRef.name || path[path.length - 1], memberDefinition];
    });
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

  resolveSymbol(cubeName, name) {
    const { sqlResolveFn, contextSymbols, collectJoinHints } = this.resolveSymbolsCallContext || {};
    if (CONTEXT_SYMBOLS[name]) {
      // always resolves if contextSymbols aren't passed for transpile step
      const symbol = contextSymbols && contextSymbols[CONTEXT_SYMBOLS[name]] || {};
      // eslint-disable-next-line no-underscore-dangle
      symbol._objectWithResolvedProperties = true;
      return symbol;
    }

    let cube = this.isCurrentCube(name) && this.symbols[cubeName] || this.symbols[name];
    if (sqlResolveFn && cube) {
      cube = this.cubeReferenceProxy(
        this.isCurrentCube(name) ? cubeName : name,
        collectJoinHints ? [] : undefined
      );
    }

    return cube || (this.symbols[cubeName] && this.symbols[cubeName][name]);
  }

  cubeReferenceProxy(cubeName, joinHints) {
    if (joinHints) {
      joinHints = joinHints.concat(cubeName);
    }
    const self = this;
    const { sqlResolveFn, cubeAliasFn, query, cubeReferencesUsed } = self.resolveSymbolsCallContext || {};
    return new Proxy({}, {
      get: (v, propertyName) => {
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
        if (propertyName === '_objectWithResolvedProperties') {
          return true;
        }
        if (cube[propertyName]) {
          return {
            toString: () => this.withSymbolsCallContext(
              () => sqlResolveFn(cube[propertyName], cubeName, propertyName),
              { ...this.resolveSymbolsCallContext, joinHints },
            ),
          };
        }
        if (self.symbols[propertyName]) {
          return this.cubeReferenceProxy(propertyName, joinHints);
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
