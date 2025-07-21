import { ErrorReporter } from './ErrorReporter';
import { CompilerInterface } from './PrepareCompiler';

export class CubeDictionary implements CompilerInterface {
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
