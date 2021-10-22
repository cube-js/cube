import { AvailableMembers } from '@cubejs-client/react';
import FlexSearch from 'flexsearch';
import { useEffect, useRef, useState } from 'react';

import useDeepMemo from '../../../hooks/deep-memo';
import { getNameMemberPairs } from '../../../shared/helpers';

export function useCubeMemberSearch(memberTypeCubeMap: AvailableMembers) {
  const flexSearch = useRef(FlexSearch.create<string>({ encode: 'advanced' }));
  const index = flexSearch.current;

  const [keys, setFilteredKeys] = useState<string[]>([]);
  const [search, setSearch] = useState<string>('');

  const indexedMembers = useDeepMemo(() => {
    const nameMemberPairs = getNameMemberPairs([
      ...memberTypeCubeMap.measures,
      ...memberTypeCubeMap.dimensions,
      ...memberTypeCubeMap.timeDimensions,
      ...memberTypeCubeMap.segments,
    ]);

    nameMemberPairs.forEach(([name, { title }]) => index.add(<any>name, title));

    return Object.fromEntries(nameMemberPairs);
  }, [memberTypeCubeMap]);

  useEffect(() => {
    let currentSearch = search;

    (async () => {
      const results = await index.search(search);

      if (currentSearch !== search) {
        return;
      }

      setFilteredKeys(results);
    })();

    return () => {
      currentSearch = '';
    };
  }, [index, search]);

  return {
    keys,
    search,
    inputProps: {
      value: search,
      onChange(event: React.ChangeEvent<HTMLInputElement>) {
        setSearch(event.target.value);
      },
    },
  };
}
