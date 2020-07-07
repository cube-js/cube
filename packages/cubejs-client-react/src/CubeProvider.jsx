import React from 'react';
import CubeContext from './CubeContext';

const CubeProvider = ({ cubejsApi, children }) => (
  <CubeContext.Provider value={{ cubejsApi }}>
    {children}
  </CubeContext.Provider>
);

export default CubeProvider;
