import { DependencyList, useMemo, useRef } from 'react';

import { useUnmountEffect } from './unmount-effect';

export function useDebouncedCallback<
  Fn extends (...args: any[]) => any,
  ResType = Promise<Awaited<ReturnType<Fn>>>,
>(callback: Fn, deps: DependencyList, delay: number, maxWait = 0): DebouncedFunction<Fn, ResType> {
  const timeout = useRef<ReturnType<typeof setTimeout>>();
  const waitTimeout = useRef<ReturnType<typeof setTimeout>>();
  const lastCall = useRef<{
    args: Parameters<Fn>;
    this: ThisParameterType<Fn>;
    resolve: (value: Awaited<ResType>) => void;
  }>();

  const clear = () => {
    if (timeout.current) {
      clearTimeout(timeout.current);
      timeout.current = undefined;
    }

    if (waitTimeout.current) {
      clearTimeout(waitTimeout.current);
      waitTimeout.current = undefined;
    }
  };

  // cancel scheduled execution on unmount
  useUnmountEffect(clear);

  return useMemo(() => {
    const execute = () => {
      if (!lastCall.current) {
        return;
      }

      const context = lastCall.current;
      lastCall.current = undefined;

      context.resolve(callback.apply(context.this, context.args));

      clear();
    };

    const wrapped = function (this, ...args) {
      return new Promise<Awaited<ResType>>((resolve, reject) => {
        if (timeout.current) {
          clearTimeout(timeout.current);
        }

        lastCall.current = { args, this: this, resolve };

        if (delay === 0) {
          execute();

          return;
        }

        // plan regular execution
        timeout.current = setTimeout(execute, delay);

        // plan maxWait execution if required
        if (maxWait > 0 && !waitTimeout.current) {
          waitTimeout.current = setTimeout(execute, maxWait);
        }
      });
    } as DebouncedFunction<Fn, ResType>;

    Object.defineProperties(wrapped, {
      length: { value: callback.length },
      name: { value: `${callback.name || 'anonymous'}__debounced__${delay}` },
    });

    return wrapped;
  }, [delay, maxWait, ...deps]);
}

export interface DebouncedFunction<
  Fn extends (...args: any[]) => any,
  ResType = Promise<Awaited<ReturnType<Fn>>>,
> {
  (this: ThisParameterType<Fn>, ...args: Parameters<Fn>): ResType;
}
