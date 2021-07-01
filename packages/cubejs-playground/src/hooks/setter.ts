import { useState } from 'react';

export function useSetter<S, T>(
  setter: (state: S, value?: T) => S,
  initialValue: S
): [S, (value?: T) => void] {
  const [state, setValue] = useState<S>(setter(initialValue));

  return [
    state,
    (value) => {
      setValue(setter(state, value));
    },
  ];
}
