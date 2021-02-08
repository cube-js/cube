import { createContext, useContext } from 'react';

export const CubeCloudContext = createContext(null);

export function CloudProvider({ children, value }) {
  return (
    <CubeCloudContext.Provider value={value}>
      {children}
    </CubeCloudContext.Provider>
  );
}

export function useCubeCloud() {
  return useContext(CubeCloudContext);
}
