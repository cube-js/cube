import {
  getCubestoreResult,
  getFinalQueryResult,
  getFinalQueryResultArray,
  getFinalQueryResultMulti,
  ResultRow
} from './index';

export interface DataResult {
  isWrapper: boolean;
  getFinalResult(): Promise<any>;
}

class BaseWrapper {
    public readonly isWrapper: boolean = true;
}

export class ResultWrapper extends BaseWrapper implements DataResult {
  private readonly proxy: any;

  private cache: any;

  public cached: Boolean = false;

  private readonly isNative: Boolean = false;

  private transformData: any;

  private rootResultObject: any = {};

  public constructor(private readonly nativeReference: any, private readonly jsResult: any = null) {
    super();

    if (nativeReference) {
      this.isNative = true;
    }

    this.proxy = new Proxy(this, {
      get: (target, prop: string | symbol) => {
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

  public getRawData() {
    if (this.isNative) {
      return this.nativeReference;
    }

    return this.jsResult;
  }

  public setTransformData(td: any) {
    this.transformData = td;
  }

  public getTransformData(): any {
    return this.transformData;
  }

  public setRootResultObject(obj: any) {
    this.rootResultObject = obj;
  }

  public getRootResultObject(): any {
    return this.rootResultObject;
  }

  public async getFinalResult(): Promise<any> {
    return getFinalQueryResult(this.transformData, this.getRawData(), this.rootResultObject);
  }
}

export class ResultMultiWrapper extends BaseWrapper implements DataResult {
  public constructor(private readonly results: ResultWrapper[], private rootResultObject: any) {
    super();
  }

  public async getFinalResult(): Promise<any> {
    const [transformDataJson, rawDataRef, cleanResultList] = this.results.reduce<[Object[], any[], Object[]]>(
      ([transformList, rawList, resultList], r) => {
        transformList.push(r.getTransformData());
        rawList.push(r.getRawData());
        resultList.push(r.getRootResultObject());
        return [transformList, rawList, resultList];
      },
      [[], [], []]
    );

    const responseDataObj = {
      queryType: this.rootResultObject.queryType,
      results: cleanResultList,
      slowQuery: this.rootResultObject.slowQuery,
    };

    return getFinalQueryResultMulti(transformDataJson, rawDataRef, responseDataObj);
  }
}

export class ResultArrayWrapper extends BaseWrapper implements DataResult {
  public constructor(private readonly results: ResultWrapper[]) {
    super();
  }

  public async getFinalResult(): Promise<any> {
    const [transformDataJson, rawData, resultDataJson] = this.results.reduce<[Object[], any[], Object[]]>(
      ([transformList, rawList, resultList], r) => {
        transformList.push(r.getTransformData());
        rawList.push(r.getRawData());
        resultList.push(r.getRootResultObject());
        return [transformList, rawList, resultList];
      },
      [[], [], []]
    );

    return getFinalQueryResultArray(transformDataJson, rawData, resultDataJson);
  }
}
