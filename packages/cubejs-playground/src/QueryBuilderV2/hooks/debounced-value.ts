import { useDebouncedState } from './debounced-state';

export function useDebouncedValue<T>(value: T, delay: number, maxWait?: number) {
  const [debouncedValue, setDebouncedValue] = useDebouncedState(value, delay, maxWait);

  if (value !== debouncedValue) {
    setDebouncedValue(value);
  }

  return debouncedValue;
}
