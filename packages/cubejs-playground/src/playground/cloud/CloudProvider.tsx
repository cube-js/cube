import { createContext, ReactNode, useContext, useState } from 'react';

import { PreAggregationDefinition } from '../../components/RollupDesigner/utils';

type AddPreAggregationToSchemaResult = {
  error?: string;
};

export type FetchRequestFromApmResult = {
  error?: string;
  request?: { duration: string; onClick: Function };
};

type CloudProviderContext = {
  isCloud: boolean;
  fetchRequestFromApm?: (
    requestId: string,
    prevRequestId?: string
  ) => Promise<FetchRequestFromApmResult>;
  addPreAggregationToSchema?: (
    preAggregationDefinition: PreAggregationDefinition
  ) => Promise<AddPreAggregationToSchemaResult>;
  setContext: (
    partialContext: Partial<Omit<CloudProviderContext, 'setContext'>>
  ) => void;
};

const CloudContext = createContext({} as CloudProviderContext);

type CloudProviderProps = {
  children: ReactNode;
};

export function CloudProvider({ children }: CloudProviderProps) {
  const [context, set] = useState<Partial<CloudProviderContext>>({});

  return (
    <CloudContext.Provider
      value={{
        ...context,
        setContext(partialContext) {
          set({
            ...context,
            ...partialContext,
          });
        },
        isCloud: true,
      }}
    >
      {children}
    </CloudContext.Provider>
  );
}

export function useCloud() {
  return useContext(CloudContext);
}
