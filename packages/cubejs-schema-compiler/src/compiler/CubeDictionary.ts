import R from 'ramda';

import type { ErrorReporter } from './ErrorReporter';

export class CubeDictionary {
  protected byId: Record<string, any>;

  public constructor() {
    this.byId = {};
  }

  // eslint-disable-next-line no-unused-vars
  public compile(cubes: any[], errorReporter: ErrorReporter) {
    this.byId = R.fromPairs(cubes.map(v => [v.name, v]));
  }

  public resolveCube(cubeName) {
    return this.byId[cubeName];
  }
}
