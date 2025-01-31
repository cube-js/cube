import { useRef } from 'react';
import equals from 'fast-deep-equal/es6';

export function useDeepCompareMemoize(value) {
  const ref = useRef([]);

  if (!equals(value, ref.current)) {
    ref.current = value;
  }

  return ref.current;
}
