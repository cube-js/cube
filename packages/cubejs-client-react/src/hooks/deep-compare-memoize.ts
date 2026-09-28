import { useRef } from 'react';
import { equals } from 'ramda';

export default function useDeepCompareMemoize(value: unknown[]): unknown[] {
  const ref = useRef<unknown[]>([]);

  if (!equals(value, ref.current)) {
    ref.current = value;
  }

  return ref.current;
}
