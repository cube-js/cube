import { TranspilerSymbolResolver } from './transpiler.interface';
import { CONTEXT_SYMBOLS, CURRENT_CUBE_CONSTANTS } from '../CubeSymbols';

type CubeSymbols = Record<string, Record<string, boolean>>;

export class LightweightSymbolResolver implements TranspilerSymbolResolver {
  public constructor(private symbols: CubeSymbols = {}) {
  }

  public setSymbols(symbols: CubeSymbols) {
    this.symbols = symbols;
  }

  public isCurrentCube(name): boolean {
    return CURRENT_CUBE_CONSTANTS.indexOf(name) >= 0;
  }

  public resolveSymbol(cubeName, name): any {
    if (name === 'USER_CONTEXT') {
      throw new Error('Support for USER_CONTEXT was removed, please migrate to SECURITY_CONTEXT.');
    }

    if (CONTEXT_SYMBOLS[name]) {
      return true;
    }

    const cube = this.symbols[this.isCurrentCube(name) ? cubeName : name];
    return cube || (this.symbols[cubeName] && this.symbols[cubeName][name]);
  }
}
