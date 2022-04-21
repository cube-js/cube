import { MaybeCancelablePromise } from '@cubejs-backend/shared';

export type SaveCancelFn = <T>(promise: MaybeCancelablePromise<T>) => Promise<T>;

export function cancelCombinator(fn) {
  const cancelFnArray = [];

  const saveCancelFn = promise => {
    if (promise.cancel) {
      cancelFnArray.push(promise.cancel);
    }
    return promise;
  };

  const promise = fn(saveCancelFn);
  promise.cancel = () => Promise.all(cancelFnArray.map(cancel => cancel()));

  return promise;
}

export class TableName {
  public constructor(
    public readonly schema: string,
    public readonly name: string,
  ) {
  }

  public static split(tableName: string): TableName {
    const parts = tableName.split('.');
    return new TableName(parts[0], parts.slice(1).join('.'));
  }

  public join() {
    return `${this.schema}.${this.name}`;
  }
}
