import { useRef } from 'react';
import equals from 'fast-deep-equal';

export default function useDeepCompareMemoize(value) {
  const ref = useRef([]);

  if (!equals(value, ref.current)) {
    ref.current = value;
  }

  return ref.current;
}
