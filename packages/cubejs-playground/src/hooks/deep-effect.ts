import { DependencyList, useEffect } from 'react';

import { useDeepDependencies } from './deep-dependencies';

export function useDeepEffect(
  callback: () => void,
  dependencies: DependencyList
) {
  const memoizedDependencies = useDeepDependencies(dependencies);
  return useEffect(callback, memoizedDependencies);
}
