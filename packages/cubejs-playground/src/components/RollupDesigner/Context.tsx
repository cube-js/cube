import { CubejsApi, Query, TransformedQuery } from '@cubejs-client/core';
import { AvailableMembers, useCubeMeta, useDryRun } from '@cubejs-client/react';
import {
  createContext,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from 'react';

import { useToggle } from '../../hooks';
import { RollupDesignerModal } from './components/RollupDesignerModal';

type RollupDesignerContextProps = {
  error: Error | null;
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
  cubejsApi?: CubejsApi;
};

export function RollupDesignerContext({
  cubejsApi,
  children,
  ...props
}: ContextProps) {
  const [isModalOpen, toggleModal] = useToggle();
  const [error, setError] = useState<Error | null>(null);
  const [query, setQuery] = useState<Query | null>(null);
  const [transformedQuery, setTransformedQuery] =
    useState<TransformedQuery | null>(null);
  const [memberTypeCubeMap, setMemberTypeCubeMap] = useState<AvailableMembers>({
    measures: [],
    dimensions: [],
    segments: [],
    timeDimensions: [],
  });

  const metaResult = useCubeMeta({
    skip: !isModalOpen,
    cubejsApi,
  });
  const dryRunResult = useDryRun(query as Query, {
    skip: !isModalOpen || !query,
    cubejsApi,
  });

  useEffect(() => {
    const { isLoading, error, response } = metaResult;

    if (!isLoading) {
      if (response) {
        setMemberTypeCubeMap(response.membersGroupedByCube());
      } else if (error) {
        setError(error);
      }
    }
  }, [metaResult.isLoading]);

  useEffect(() => {
    const { isLoading, error, response } = dryRunResult;

    if (!isLoading) {
      if (response) {
        setTransformedQuery(response.transformedQueries[0]);
      } else if (error) {
        setError(error);
      }
    }
  }, [dryRunResult.isLoading]);

  function reset() {
    setTransformedQuery(null);
    setError(null);
  }

  return (
    <Context.Provider
      value={{
        isLoading: metaResult.isLoading || dryRunResult.isLoading,
        isModalOpen,
        toggleModal,
        query,
        setQuery,
        transformedQuery,
        setTransformedQuery,
        memberTypeCubeMap,
        error,
      }}
    >
      {children}

      <RollupDesignerModal apiUrl={props.apiUrl} onAfterClose={reset} />
    </Context.Provider>
  );
}

export function useRollupDesignerContext() {
  return useContext(Context);
}
