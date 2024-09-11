import { useCallback } from 'react';

import { useSyncRef } from './sync-ref';

/**
 * useEvent shim from the latest React RFC.
 *
 * @see https://github.com/reactjs/rfcs/pull/220
 * @see https://github.com/reactjs/rfcs/blob/useevent/text/0000-useevent.md#internal-implementation
 */
export function useEvent<
  Func extends (...args: Args) => Result,
  Args extends Parameters<any> = Parameters<Func>,
  Result extends ReturnType<any> = ReturnType<Func>,
>(callback: Func): (...args: Args) => Result {
  const callbackRef = useSyncRef(callback);

  return useCallback((...args) => callbackRef.current(...args), []);
}
