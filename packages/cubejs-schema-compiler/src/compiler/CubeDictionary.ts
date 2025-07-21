import { ErrorReporter } from './ErrorReporter';

export class CubeDictionary {
  public byId: Record<string, any>;

  public constructor() {
    this.byId = {};
  }

  public compile(cubes: any[], _errorReporter: ErrorReporter) {
    this.byId = Object.fromEntries(cubes.map(v => [v.name, v]));
  }

  public resolveCube(cubeName: string): any {
    return this.byId[cubeName];
  }
}
