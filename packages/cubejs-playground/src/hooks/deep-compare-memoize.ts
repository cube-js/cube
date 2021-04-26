import { useRef } from 'react';
import equals from 'fast-deep-equal';

export function useDeepCompareMemoize(value) {
  const ref = useRef([]);

  if (!equals(value, ref.current)) {
    ref.current = value;
  }

  return ref.current;
}
