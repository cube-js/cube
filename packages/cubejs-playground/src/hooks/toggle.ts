import { useState } from 'react';

export function useToggle(defaultValue: boolean = false): [boolean, (...args: any) => void] {
  const [isOn, toggle] = useState(defaultValue);

  return [isOn, () => toggle((value) => !value)];
}
