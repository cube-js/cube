import { Dispatch, SetStateAction, useRef, useState } from 'react';

import { useDebouncedCallback } from './debounced-callback';
import { useEvent } from './event';

export function useDebouncedState<S>(
  initialState: S | (() => S),
  delay: number,
  maxWait = 0
): [S, Dispatch<SetStateAction<S>>] {
  const [state, setState] = useState(initialState);
  const currentValueRef = useRef(state);

  const dSetState = useDebouncedCallback(setState, [], delay, maxWait);
  const setValue = useEvent((next: S | ((currentValue: S) => S)) => {
    currentValueRef.current = next instanceof Function ? next(currentValueRef.current) : next;

    void dSetState(currentValueRef.current);
  });

  return [state, setValue];
}
