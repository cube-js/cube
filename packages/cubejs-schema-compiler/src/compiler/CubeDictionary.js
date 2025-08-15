import R from 'ramda';

export class CubeDictionary {
  constructor() {
    this.byId = {};
  }

  // eslint-disable-next-line no-unused-vars
  compile(cubes, errorReporter) {
    this.byId = R.fromPairs(cubes.map(v => [v.name, v]));
  }

  resolveCube(cubeName) {
    return this.byId[cubeName];
  }

  free() {
    this.byId = {};
  }
}
