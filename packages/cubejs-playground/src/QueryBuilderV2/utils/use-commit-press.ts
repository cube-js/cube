import { useEffect } from 'react';

export const useCommitPress = (callback?: (event: KeyboardEvent) => void, prevent = false) => {
  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      // Check for Cmd+Enter on macOS or Ctrl+Enter on other platforms
      if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
        if (prevent) {
          event.preventDefault();
          event.stopPropagation();
        }

        callback?.(event);
      }
    };

    // Attach the event listener with capture: true
    window.addEventListener('keydown', handler, { capture: true });

    // Cleanup function to remove the event listener
    return () => {
      window.removeEventListener('keydown', handler, { capture: true });
    };
  }, [callback]); // Only re-run effect if callback changes
};
