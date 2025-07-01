import { useEffect, useMemo, useState } from 'react';
import mitt from 'mitt';

import { useEvent } from './event';

const storage = new (class Storage {
  emitter = mitt();

  constructor() {
    window.addEventListener('storage', (event) => {
      if (event.key == null) {
        this.emitter.emit('clear');

        return;
      }

      if (event.oldValue !== event.newValue) {
        let value;

        try {
          value = JSON.parse(event.newValue || '');
        } catch (error: any) {
          value = event.newValue;
        }
        event.key && this.emit(event.key, value);
      }
    });
  }

  emit(key: string, value?: any) {
    this.emitter.emit(key, value);
  }

  subscribe(key: string, callback: any) {
    this.emitter.on(key, callback);
  }

  unsubscribe(key: string, callback: any) {
    this.emitter.off(key, callback);
  }

  onClear(callback: any) {
    this.emitter.on('clear', callback);
  }

  onClearUnsubscribe(callback: any) {
    this.emitter.off('clear', callback);
  }

  setItem(key: string, value: any) {
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
    } catch (error: any) {
      return typeof defaultValue === 'function' ? defaultValue(null) : defaultValue;
    }
  }

  removeItem(key: string) {
    this.setItem(key, undefined);
  }
})();

export function useLocalStorage<T = any>(
  key: string,
  defaultValue?: T | ((value: unknown) => T)
): [T, (next: T) => T, () => void] {
  const getter = () => storage.getItem(key, defaultValue);

  const [value, setValue] = useState<T>(getter);

  useEffect(() => {
    setValue(getter);

    storage.subscribe(key, (value: T) => {
      // @ts-ignore
      setValue(typeof defaultValue === 'function' ? defaultValue(value) : (value ?? defaultValue));
    });

    return () => {
      storage.unsubscribe(key, setValue);
    };
  }, [key]);

  const handleClear = useEvent(() => {
    // @ts-ignore
    setValue(() => null);
  });

  useEffect(() => {
    storage.onClear(handleClear);

    return () => {
      return storage.onClearUnsubscribe(handleClear);
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
