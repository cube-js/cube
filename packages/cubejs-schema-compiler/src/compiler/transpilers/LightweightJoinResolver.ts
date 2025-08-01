import { TranspilerSymbolResolver } from './transpiler.interface';
import { CONTEXT_SYMBOLS, CURRENT_CUBE_CONSTANTS } from '../CubeSymbols';

export class LightweightJoinResolver implements TranspilerSymbolResolver {
  public constructor(private cubeJoinAliases: Record<string, Record<string, boolean>> = {}) {
  }

  public setJoinAliases(cubeJoinAliases: Record<string, Record<string, boolean>>) {
    this.cubeJoinAliases = cubeJoinAliases;
  }

  public isCurrentCube(name: string): boolean {
    return CURRENT_CUBE_CONSTANTS.indexOf(name) >= 0;
  }

  public resolveSymbol(cubeName: string, name: string): any {
    if (name === 'USER_CONTEXT') {
      throw new Error('Support for USER_CONTEXT was removed, please migrate to SECURITY_CONTEXT.');
    }

    if (CONTEXT_SYMBOLS[name]) {
      return true;
    }

    const cube = this.cubeJoinAliases[this.isCurrentCube(name) ? cubeName : name];
    return !!(cube || this.cubeJoinAliases[cubeName]?.[name]);
  }
}
