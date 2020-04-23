exports.cancelCombinator = (fn) => {
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
};
