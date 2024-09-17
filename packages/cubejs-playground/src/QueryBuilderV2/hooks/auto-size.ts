import { useState, useEffect, RefObject } from 'react';

import { useEvent } from './event';
import { useWindowSize } from './window-size';

function useWindowUpdate(container: HTMLElement | null, callback: () => void): void {
  const windowSize = useWindowSize();

  useEffect(() => {
    if (container) {
      // Wait for the container position update
      const id = requestAnimationFrame(callback);

      window.addEventListener('scroll', callback, { passive: true });

      return () => {
        cancelAnimationFrame(id);
        window.removeEventListener('scroll', callback);
      };
    }
  }, [windowSize.height, container]);
}

type AutoSizeData = {
  value: number;
  cssValue: string;
  cssCalc: string;
};

export function useAutoSizeData<T extends HTMLElement>(
  ref: RefObject<T>,
  compensation = 0
): [AutoSizeData, () => void] {
  const [sizeData, setSizeData] = useState<AutoSizeData>({
    value: 0,
    cssValue: '0px',
    cssCalc: '0px',
  });
  const container = ref.current;

  const updateSize = useEvent(() => {
    if (container) {
      const height =
        window.innerHeight -
        container.getBoundingClientRect().y +
        compensation -
        document.documentElement.scrollTop;

      const value = Math.max(Math.min(height, window.innerHeight), 0);

      setSizeData({
        value,
        cssValue: `${value}px`,
        cssCalc: `calc(100vh - ${container.getBoundingClientRect().y - compensation}px)`,
      });
    }
  });

  useEffect(() => {
    updateSize();
  }, [compensation]);

  useWindowUpdate(container, updateSize);

  return [sizeData, updateSize];
}

export function useAutoSizeCalc<T extends HTMLElement>(
  ref: RefObject<T>,
  compensation = 0
): [string, () => void] {
  const [containerSize, setContainerSize] = useState<string>('0px');
  const container = ref.current;

  const updateSize = useEvent(() => {
    if (container) {
      setContainerSize(`calc(100vh - ${container.getBoundingClientRect().y - compensation}px)`);
    }
  });

  useWindowUpdate(container, updateSize);

  return [containerSize, updateSize];
}

export function useAutoSize<T extends HTMLElement>(
  ref: RefObject<T>,
  compensation = 0
): [number, () => void] {
  const [containerSize, setContainerSize] = useState(0);
  const container = ref.current;

  const updateSize = useEvent(() => {
    if (container) {
      const height =
        window.innerHeight -
        container.getBoundingClientRect().y +
        compensation -
        document.documentElement.scrollTop;

      setContainerSize(Math.max(Math.min(height, window.innerHeight), 0));
    }
  });

  useEffect(() => {
    updateSize();
  }, [compensation]);

  useWindowUpdate(container, updateSize);

  return [containerSize, updateSize];
}

export function useAutoSizePx<T extends HTMLElement>(
  ref: RefObject<T>,
  compensation = 0
): [string, () => void] {
  const [containerSize, updateSize] = useAutoSize(ref, compensation);

  return [containerSize === 0 ? 'auto' : `${containerSize}px`, updateSize];
}
