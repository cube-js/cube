import { DependencyList, useMemo } from 'react';

import { useDeepDependencies } from './deep-dependencies';

export default function useDeepMemo<T>(
  callback: () => T,
  dependencies: DependencyList
) {
  const memoizedDependencies = useDeepDependencies(dependencies);
  return useMemo<T>(callback, memoizedDependencies);
}
