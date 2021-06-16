import { useState } from 'react';

export function useSetter<T = undefined>(
  setter: (value: T | undefined) => T | undefined,
  initialValue?: T
): [T | undefined, (value: T | undefined) => void] {
  const [value, setValue] = useState(setter(initialValue));

  return [
    value,
    (value) => {
      setValue(setter(value));
    },
  ];
}
