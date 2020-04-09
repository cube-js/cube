const R = require('ramda');
const UserError = require('./UserError');
const DynamicReference = require('./DynamicReference');

const FunctionRegex = /function\s+\w+\(([A-Za-z0-9_,]*)|\(([\s\S]*?)\)\s*=>|\(?(\w+)\)?\s*=>/;
const CONTEXT_SYMBOLS = {
  USER_CONTEXT: 'userContext',
  FILTER_PARAMS: 'filterParams',
  SQL_UTILS: 'sqlUtils'
};

const CURRENT_CUBE_CONSTANTS = ['CUBE', 'TABLE'];

class CubeSymbols {
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
    const cubeObject = Object.assign({
      allDefinitions(type) {
        let superDefinitions = {};
        if (cubeDefinition.extends) {
          superDefinitions = super.allDefinitions(type);
        }
        return Object.assign({}, superDefinitions, cubeDefinition[type]);
      },
      get measures() {
        return this.allDefinitions('measures');
      },
      // eslint-disable-next-line no-empty-function
      set measures(v) {},

      get dimensions() {
        return this.allDefinitions('dimensions');
      },
      // eslint-disable-next-line no-empty-function
      set dimensions(v) {},

      get segments() {
        return this.allDefinitions('segments');
      },
      // eslint-disable-next-line no-empty-function
      set segments(v) {}
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
    )([cube.measures, cube.dimensions, cube.segments]);
    if (duplicateNames.length > 0) {
      errorReporter.error(`${duplicateNames.join(', ')} defined more than once`);
    }
    return Object.assign(
      { cubeName: () => cube.name }, cube.measures || {}, cube.dimensions || {}, cube.segments || {}
    );
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

  resolveSymbol(cubeName, name) {
    // eslint-disable-next-line no-unused-vars
    const { sqlResolveFn, contextSymbols, query } = this.resolveSymbolsCallContext || {};
    if (CONTEXT_SYMBOLS[name]) {
      // always resolves if contextSymbols aren't passed for transpile step
      const symbol = contextSymbols && contextSymbols[CONTEXT_SYMBOLS[name]] || {};
      // eslint-disable-next-line no-underscore-dangle
      symbol._objectWithResolvedProperties = true;
      return symbol;
    }
    let cube = this.isCurrentCube(name) && this.symbols[cubeName] || this.symbols[name];
    if (sqlResolveFn && cube) {
      cube = this.cubeReferenceProxy(this.isCurrentCube(name) ? cubeName : name);
    }
    return cube || (this.symbols[cubeName] && this.symbols[cubeName][name]);
  }

  cubeReferenceProxy(cubeName) {
    const self = this;
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
        const { sqlResolveFn, cubeAliasFn, query } = self.resolveSymbolsCallContext || {};
        if (propertyName === 'toString') {
          return () => cubeAliasFn && cubeAliasFn(cube.cubeName()) || cube.cubeName();
        }
        if (propertyName === 'sql') {
          return () => query.cubeSql(cube.cubeName());
        }
        if (propertyName === '_objectWithResolvedProperties') {
          return true;
        }
        if (cube[propertyName]) {
          return { toString: () => sqlResolveFn(cube[propertyName], cubeName, propertyName) };
        }
        if (typeof propertyName === 'string') {
          throw new UserError(`${cubeName}.${propertyName} cannot be resolved`);
        }
        return undefined;
      }
    });
  }

  isCurrentCube(name) {
    return CURRENT_CUBE_CONSTANTS.indexOf(name) >= 0;
  }
}

module.exports = CubeSymbols;
