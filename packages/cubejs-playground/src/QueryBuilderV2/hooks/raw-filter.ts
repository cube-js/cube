import { useMemo } from 'react';

import { contains } from '../utils/contains';

import { useEvent } from './event';

export function useRawFilter() {
  const collator = useMemo(
    () =>
      new Intl.Collator('US-en', {
        usage: 'search',
        sensitivity: 'base',
        ignorePunctuation: true,
        localeMatcher: 'lookup',
      }),
    []
  );

  return useEvent((textValue: string, currentInputValue: string) => {
    if (!currentInputValue) {
      return true;
    }

    return contains(textValue.toLowerCase(), currentInputValue.toLowerCase(), collator.compare);
  });
}
