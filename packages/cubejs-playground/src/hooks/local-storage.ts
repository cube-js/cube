import { useEffect, useMemo, useState } from 'react';
import mitt from 'mitt';

import { useIdentifier } from './identifier';

const storage = new (class Storage {
  emitter = mitt();

  constructor() {
    window.addEventListener('storage', (event) => {
      if (event.oldValue !== event.newValue) {
        let value;

        try {
          value = JSON.parse(event.newValue || '');
        } catch {
          value = event.newValue;
        }
        this.emit(event.key, value);
      }
    });
  }

  emit(key, value) {
    this.emitter.emit(key, value);
  }

  subscribe(key, callback) {
    this.emitter.on(key, callback);
  }

  unsubscribe(key, callback) {
    this.emitter.off(key, callback);
  }

  setItem(key, value) {
    if (value === undefined) {
      localStorage.removeItem(key);
    } else {
      const data = JSON.stringify(value);
      localStorage.setItem(key, data);
    }

    this.emit(key, value);
  }

  getItem(key: string, defaultValue: any) {
    try {
      const data = localStorage.getItem(key);
      const parsed = typeof data === 'string' ? JSON.parse(data) : undefined;

      if (typeof defaultValue === 'function') {
        return defaultValue(parsed);
      }

      return parsed !== undefined ? parsed : defaultValue;
    } catch {
      return typeof defaultValue === 'function'
        ? defaultValue(null)
        : defaultValue;
    }
  }

  removeItem(key: string) {
    this.setItem(key, undefined);
  }
})();

export function useLocalStorage<T = any>(
  itemKey: string,
  defaultValue?: T | ((value: unknown) => T)
): [T, (next: T) => T, () => void] {
  const identifier = useIdentifier();

  const key = [itemKey, identifier ? ':' : '', identifier].join('');
  const getter = () => storage.getItem(key, defaultValue);

  const [value, setValue] = useState<T>(getter);

  useEffect(() => {
    setValue(getter);

    storage.subscribe(key, (nextValue) => {
      // @ts-ignore
      setValue(typeof defaultValue === 'function' ? defaultValue(nextValue) : nextValue || defaultValue);
    });

    return () => {
      storage.unsubscribe(key, setValue);
    };
  }, [key]);

  return useMemo(() => {
    function setItem(next: T) {
      storage.setItem(key, next);

      return next;
    }

    function removeItem() {
      storage.removeItem(key);
    }

    return [value, setItem, removeItem];
  }, [key, value]);
}
