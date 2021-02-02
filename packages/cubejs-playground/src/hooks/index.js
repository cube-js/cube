import { createContext } from 'react';

export const AppContext = createContext({
  slowQuery: false,
  isPreAggregationBuildInProgress: false
});