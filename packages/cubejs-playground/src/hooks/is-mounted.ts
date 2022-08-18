import { useCallback, useEffect, useRef } from 'react';

export function useIsMounted() {
  const isMountedRef = useRef(false);
  const isMounted = useCallback(() => isMountedRef.current, []);

  useEffect(() => {
    isMountedRef.current = true;

    return () => {
      isMountedRef.current = false;
    };
  }, []);

  return isMounted;
}
