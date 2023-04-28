import R from 'ramda';
import { getEnv } from '@cubejs-backend/shared';
import { camelize } from 'inflection';

import { UserError } from './UserError';
import { DynamicReference } from './DynamicReference';
import { camelizeCube } from './utils';

const FunctionRegex = /function\s+\w+\(([A-Za-z0-9_,]*)|\(([\s\S]*?)\)\s*=>|\(?(\w+)\)?\s*=>/;
const CONTEXT_SYMBOLS = {
  USER_CONTEXT: 'securityContext',
  SECURITY_CONTEXT: 'securityContext',
  FILTER_PARAMS: 'filterParams',
  SQL_UTILS: 'sqlUtils'
};

const CURRENT_CUBE_CONSTANTS = ['CUBE', 'TABLE'];

export class CubeSymbols {
  constructor() {
    this.symbols = {};
    this.builtCubes = {};
    this.cubeDefinitions = {};
    this.funcArgumentsValues = {};
    this.cubeList = [];
  }

  compile(cubes, errorReporter) {
    this.cubeDefinitions = R.pipe(
      R.map(c => [c.name, c]),
      R.fromPairs
    )(cubes);
    this.cubeList = cubes.map(c => (c.name ? this.getCubeDefinition(c.name) : this.createCube(c)));
    this.symbols = R.pipe(
      R.map((c) => [c.name, this.transform(c.name, errorReporter.inContext(`${c.name} cube`))]),
      R.fromPairs
    )(cubes);
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

  transform(cubeName, errorReporter) {
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
