import { useEffect } from 'react';

import { useEvent } from './event';

/**
 * Calls callback when component is unmounted.
 */
export function useUnmountEffect(callback: () => void) {
  // by using `useEvent` we can ensure that the callback is stable
  const callbackEvent = useEvent(callback);

  useEffect(() => callbackEvent, []);
}
