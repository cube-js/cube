import { useEffect, useRef } from 'react';

export function useIsMounted() {
  const isMounted = useRef(true);

  useEffect(() => {
    return () => {
      isMounted.current = false;
    };
  }, []);

  return () => isMounted.current;
}
