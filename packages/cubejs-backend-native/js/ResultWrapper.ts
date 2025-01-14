import {
  getCubestoreResult,
  getFinalQueryResult,
  getFinalQueryResultMulti,
  ResultRow
} from './index';

export interface DataResult {
  isWrapper: boolean;
  getFinalResult(): Promise<any>;
  getRawData(): any[];
  getTransformData(): any[];
  getRootResultObject(): any[];
  // eslint-disable-next-line no-use-before-define
  getResults(): ResultWrapper[];
}

class BaseWrapper {
  public readonly isWrapper: boolean = true;
}

export class ResultWrapper extends BaseWrapper implements DataResult {
  private readonly proxy: any;

  private cache: any;

  public cached: Boolean = false;

  private readonly isNative: Boolean = false;

  private readonly nativeReference: any;

  private readonly jsResult: any = null;

  private transformData: any;

  private rootResultObject: any = {};

  public constructor(input: any) {
    super();

    if (input.isWrapper) {
      return input;
    }

    if (Array.isArray(input)) {
      this.jsResult = input;
    } else {
      this.isNative = true;
      this.nativeReference = input;
    }

    this.proxy = new Proxy(this, {
      get: (target, prop: string | symbol) => {
        // To support iterative access
        if (prop === Symbol.iterator) {
          const array = this.getArray();
          const l = array.length;

          return function* yieldArrayItem() {
            for (let i = 0; i < l; i++) {
              yield array[i];
            }
          };
        }

        // intercept indexes
        if (typeof prop === 'string' && !Number.isNaN(Number(prop))) {
          const array = this.getArray();
          return array[Number(prop)];
        }

        // intercept isNative
        if (prop === 'isNative') {
          return this.isNative;
        }

        // intercept array props and methods
        if (typeof prop === 'string' && prop in Array.prototype) {
          const arrayMethod = (Array.prototype as any)[prop];
          if (typeof arrayMethod === 'function') {
            return (...args: any[]) => this.invokeArrayMethod(prop, ...args);
          }

          return (this.getArray() as any)[prop];
        }

        // intercept JSON.stringify or toJSON()
        if (prop === 'toJSON') {
          return () => this.getArray();
        }

        return (target as any)[prop];
      },

      // intercept array length
      getOwnPropertyDescriptor: (target, prop) => {
        if (prop === 'length') {
          const array = this.getArray();
          return {
            configurable: true,
            enumerable: true,
            value: array.length,
            writable: false
          };
        }
        return Object.getOwnPropertyDescriptor(target, prop);
      },

      ownKeys: (target) => {
        const array = this.getArray();
        return [...Object.keys(target), ...Object.keys(array), 'length', 'isNative'];
      }
    });
    Object.setPrototypeOf(this.proxy, ResultWrapper.prototype);

    return this.proxy;
  }

  private getArray(): ResultRow[] {
    if (!this.cache) {
      if (this.isNative) {
        this.cache = getCubestoreResult(this.nativeReference);
      } else {
        this.cache = this.jsResult;
      }
      this.cached = true;
    }
    return this.cache;
  }

  private invokeArrayMethod(method: string, ...args: any[]): any {
    const array = this.getArray();
    return (array as any)[method](...args);
  }

  public getRawData(): any[] {
    if (this.isNative) {
      return [this.nativeReference];
    }

    return [this.jsResult];
  }

  public setTransformData(td: any) {
    this.transformData = td;
  }

  public getTransformData(): any[] {
    return [this.transformData];
  }

  public setRootResultObject(obj: any) {
    this.rootResultObject = obj;
  }

  public getRootResultObject(): any[] {
    return [this.rootResultObject];
  }

  public async getFinalResult(): Promise<any> {
    return getFinalQueryResult(this.transformData, this.getRawData()[0], this.rootResultObject);
  }

  public getResults(): ResultWrapper[] {
    return [this];
  }
}

class BaseWrapperArray extends BaseWrapper {
  public constructor(protected readonly results: ResultWrapper[]) {
    super();
  }

  protected getInternalDataArrays(): any[] {
    const [transformDataJson, rawData, resultDataJson] = this.results.reduce<[Object[], any[], Object[]]>(
      ([transformList, rawList, resultList], r) => {
        transformList.push(r.getTransformData()[0]);
        rawList.push(r.getRawData()[0]);
        resultList.push(r.getRootResultObject()[0]);
        return [transformList, rawList, resultList];
      },
      [[], [], []]
    );

    return [transformDataJson, rawData, resultDataJson];
  }

  // Is invoked from the native side to get
  // an array of all raw wrapped results
  public getResults(): ResultWrapper[] {
    return this.results;
  }

  public getTransformData(): any[] {
    return this.results.map(r => r.getTransformData()[0]);
  }

  public getRawData(): any[] {
    return this.results.map(r => r.getRawData()[0]);
  }

  public getRootResultObject(): any[] {
    return this.results.map(r => r.getRootResultObject()[0]);
  }
}

export class ResultMultiWrapper extends BaseWrapperArray implements DataResult {
  public constructor(results: ResultWrapper[], private rootResultObject: any) {
    super(results);
  }

  public async getFinalResult(): Promise<any> {
    const [transformDataJson, rawDataRef, cleanResultList] = this.getInternalDataArrays();

    const responseDataObj = {
      queryType: this.rootResultObject.queryType,
      results: cleanResultList,
      slowQuery: this.rootResultObject.slowQuery,
    };

    return getFinalQueryResultMulti(transformDataJson, rawDataRef, responseDataObj);
  }
}

// This is consumed by native side via Transport Bridge
export class ResultArrayWrapper extends BaseWrapperArray implements DataResult {
  public constructor(results: ResultWrapper[]) {
    super(results);
  }

  public async getFinalResult(): Promise<any> {
    const [transformDataJson, rawDataRef, cleanResultList] = this.getInternalDataArrays();

    return [transformDataJson, rawDataRef, cleanResultList];
  }
}
