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
