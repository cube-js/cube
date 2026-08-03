import { RefObject, useEffect, useState } from 'react';

export function useHasOverflow(ref?: RefObject<HTMLDivElement>) {
  const [hasOverflow, setHasOverflow] = useState(false);

  useEffect(() => {
    const element = ref?.current;
    const handler = () => {
      const hasOverflow = (element?.scrollWidth ?? 0) > (element?.clientWidth ?? 0);

      setHasOverflow(hasOverflow);
    };

    if (!ref) {
      setHasOverflow(false);
    }

    element?.addEventListener('mouseenter', handler);

    return () => {
      element?.removeEventListener('mouseenter', handler);
    };
  }, [ref?.current]);

  return hasOverflow;
}
