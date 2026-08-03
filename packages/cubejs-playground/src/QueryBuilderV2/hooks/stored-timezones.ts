import { useEffect } from 'react';

import { useLocalStorage } from './local-storage';

export function useStoredTimezones(timezone?: string) {
  timezone = timezone ?? '';

  const [storedTimezones, setStoredTimezones] = useLocalStorage<string[]>(
    'QueryBuilder:timezones',
    []
  );

  useEffect(() => {
    // remove the timezone from stored
    const newStoredTimezones = storedTimezones.filter((value) => value !== timezone);

    // place the timezone at the top of the list
    newStoredTimezones.unshift(timezone ?? '');

    // limit the amount of stored timezones
    while (newStoredTimezones.length > 5) {
      newStoredTimezones.pop();
    }

    setStoredTimezones([...newStoredTimezones]);
  }, [timezone]);

  return storedTimezones;
}
