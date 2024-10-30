import { useCallback, useEffect, useRef } from 'react';
import { unstable_batchedUpdates } from 'react-dom';

import { useEvent } from './event';

type IntervalID = ReturnType<typeof setInterval>;

export function useIntervalEffect(
  callback: () => void,
  ms: number,
  immediate: boolean = false
): [cancel: () => void, restart: () => void] {
  const callbackEvent = useEvent(() => unstable_batchedUpdates(callback));
  const intervalIdRef = useRef<IntervalID | null>(null);

  const cancel = useCallback(() => {
    if (intervalIdRef.current !== null) {
      clearInterval(intervalIdRef.current);
      intervalIdRef.current = null;
    }
  }, []);

  const restart = useEvent(() => {
    cancel();

    if (immediate) {
      callbackEvent();
    }

    intervalIdRef.current = setInterval(callbackEvent, ms);
  });

  useEffect(() => {
    restart();

    return cancel;
  }, [ms]);

  return [cancel, restart];
}
