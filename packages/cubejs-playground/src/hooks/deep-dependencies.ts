import deepEquals from 'fast-deep-equal';
import { DependencyList, useRef } from 'react';

export function useDeepDependencies(dependencies: DependencyList) {
  const memo = useRef<DependencyList>();

  if (!deepEquals(memo.current, dependencies)) {
    memo.current = dependencies;
  }

  return memo.current;
}
