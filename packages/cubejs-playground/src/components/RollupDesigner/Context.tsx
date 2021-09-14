import { Query, TransformedQuery } from '@cubejs-client/core';
import {
  AvailableMembers,
  useCubeMeta,
  useLazyDryRun,
} from '@cubejs-client/react';
import {
  createContext,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from 'react';

import { useDeepEffect, useToggle } from '../../hooks';
import { RollupDesignerModal } from './components/RollupDesignerModal';

type RollupDesignerContextProps = {
  isLoading: boolean;
  isModalOpen: boolean;
  toggleModal: (isOpen?: boolean) => void;
  memberTypeCubeMap: AvailableMembers;
  query: Query | null;
  setQuery: (query: Query | null) => void;
  transformedQuery: TransformedQuery | null;
  setTransformedQuery: (transformedQuery: TransformedQuery | null) => void;
};

export const Context = createContext<RollupDesignerContextProps>(
  {} as RollupDesignerContextProps
);

type ContextProps = {
  apiUrl: string;
  children: ReactNode;
};

export function RollupDesignerContext({ children, ...props }: ContextProps) {
  const [isModalOpen, toggleModal] = useToggle();
  const [query, setQuery] = useState<Query | null>(null);
  const [transformedQuery, setTransformedQuery] =
    useState<TransformedQuery | null>(null);
  const [memberTypeCubeMap, setMemberTypeCubeMap] = useState<AvailableMembers>({
    measures: [],
    dimensions: [],
    segments: [],
    timeDimensions: [],
  });

  const { isLoading: isMetaLoading, response: meta } = useCubeMeta();
  const [
    fetchDryRun,
    { isLoading: isDryRunLoading, response: dryRunResponse },
  ] = useLazyDryRun();

  useDeepEffect(() => {
    if (isModalOpen && query) {
      fetchDryRun({ query });
    }
  }, [isModalOpen, query]);

  useEffect(() => {
    if (!isMetaLoading && meta) {
      setMemberTypeCubeMap(meta.membersGroupedByCube());
    }
  }, [isMetaLoading, meta]);

  useEffect(() => {
    if (!isDryRunLoading && dryRunResponse) {
      setTransformedQuery(dryRunResponse.transformedQueries[0]);
    }
  }, [isDryRunLoading, dryRunResponse]);

  return (
    <Context.Provider
      value={{
        isLoading: isMetaLoading || isDryRunLoading,
        isModalOpen,
        toggleModal,
        query,
        setQuery,
        transformedQuery,
        setTransformedQuery,
        memberTypeCubeMap,
      }}
    >
      {children}
      <RollupDesignerModal
        apiUrl={props.apiUrl}
        onAfterClose={() => setTransformedQuery(null)}
      />
    </Context.Provider>
  );
}

export function useRollupDesignerContext() {
  return useContext(Context);
}
