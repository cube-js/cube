import { createContext, useContext } from 'react';

import { QueryBuilderContextProps } from './types';

export const QueryBuilderContext = createContext<QueryBuilderContextProps | null>(null);

export function useQueryBuilderContext() {
  const context = useContext(QueryBuilderContext);

  if (!context) {
    throw new Error('useQueryBuilderContext must be used within QueryBuilderProvider');
  }

  return context;
}
