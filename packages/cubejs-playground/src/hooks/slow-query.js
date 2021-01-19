import { useContext, createContext } from 'react';

export const AppContext = createContext({
  slowQuery: false,
});

export default function useSlowQuery() {
  const { slowQuery } = useContext(AppContext);

  return slowQuery;
}
