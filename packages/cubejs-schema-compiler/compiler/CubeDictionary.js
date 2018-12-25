const R = require('ramda');

class CubeDictionary {
  constructor() {
    this.byId = {};
  }

  compile(cubes, errorReporter) {
    this.byId = R.fromPairs(cubes.map(v => [v.name, v]));
  }

  resolveCube(cubeName) {
    return this.byId[cubeName];
  }
}

module.exports = CubeDictionary;