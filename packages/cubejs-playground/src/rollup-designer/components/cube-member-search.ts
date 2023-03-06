import { AvailableMembers } from '@cubejs-client/react';
import FlexSearch from 'flexsearch';
import { useEffect, useRef, useState } from 'react';
import { useDeepEffect } from '../../hooks';
import { getNameMemberPairs } from '../../shared/members';

export function useCubeMemberSearch(memberTypeCubeMap: AvailableMembers) {
  const index = useRef(new FlexSearch.Index({ tokenize: 'forward' })).current;

  const [keys, setFilteredKeys] = useState<string[]>([]);
  const [search, setSearch] = useState<string>('');

  useDeepEffect(() => {
    const nameMemberPairs = getNameMemberPairs([
      ...memberTypeCubeMap.measures,
      ...memberTypeCubeMap.dimensions,
      ...memberTypeCubeMap.timeDimensions,
      ...memberTypeCubeMap.segments,
    ]);

    nameMemberPairs.forEach(([name, { title }]) => index.add(<any>name, title));
  }, [memberTypeCubeMap]);

  useEffect(() => {
    let currentSearch = search;

    (async () => {
      const results = await index.search(search);

      if (currentSearch !== search) {
        return;
      }

      setFilteredKeys(results as string[]);
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
