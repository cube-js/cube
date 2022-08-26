import { createContext, ReactNode, useContext, useState } from 'react';

import { PreAggregationDefinition } from '../../components/RollupDesigner/utils';

type AddPreAggregationToSchemaResult = {
  error?: string;
};

type CloudProviderContext = {
  isCloud: boolean;
  isAddRollupButtonVisible: boolean;
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
  isAddRollupButtonVisible?: boolean;
};

export function CloudProvider({ children, isAddRollupButtonVisible = true }: CloudProviderProps) {
  const [context, set] = useState<Partial<CloudProviderContext>>({});

  return (
    <CloudContext.Provider
      value={{
        isAddRollupButtonVisible,
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
