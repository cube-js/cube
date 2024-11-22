import { useEffect, RefObject } from 'react';

export function useOutsideFocus(
  ref: RefObject<HTMLElement | undefined>,
  callback: () => void
): void {
  useEffect(() => {
    function handleFocus(event: FocusEvent) {
      // Check if the focus is outside the ref
      if (ref.current && !ref.current.contains(event.target as Node)) {
        callback();
      }
    }

    // Bind the focus and blur handlers
    document.addEventListener('focus', handleFocus, true);
    document.addEventListener('blur', handleFocus, true);
    document.addEventListener('mousedown', handleFocus, true);

    return () => {
      // Unbind the focus and blur handlers when the component unmounts
      document.removeEventListener('focus', handleFocus, true);
      document.removeEventListener('blur', handleFocus, true);
      document.removeEventListener('mousedown', handleFocus, true);
    };
  }, [ref, callback]); // Re-run if ref or callback changes
}
