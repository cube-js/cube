const R = require('ramda');
const DynamicReference = require('../compiler/DynamicReference');

class RefreshKeys {
  constructor(cubeFactory, compiler) {
    this.cubeFactory = cubeFactory;
    this.compiler = compiler;
    this.dynRef = this.dynRef.bind(this);
  }

  dynRef(...args) {
    if (args.length < 2) {
      throw new Error(`List of references and a function are expected in form: dynRef('ref', (r) => (...))`);
    }
    const references = R.dropLast(1, args);
    const fn = args[args.length - 1];
    if (typeof fn !== 'function') {
      throw new Error(`Last argument should be a function`);
    }
    return new DynamicReference(references, fn);
  }
}

module.exports = RefreshKeys;
