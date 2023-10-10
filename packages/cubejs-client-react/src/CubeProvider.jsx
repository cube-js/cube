import React from 'react';
import CubeContext from './CubeContext';

export default function CubeProvider({ cubejsApi, children, options = {} }) {
  return (
    <CubeContext.Provider value={{
      cubejsApi,
      options
    }}
    >
      {children}
    </CubeContext.Provider>
  );
}
