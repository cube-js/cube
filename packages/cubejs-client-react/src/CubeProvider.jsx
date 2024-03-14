import React from 'react';
import CubeContext from './CubeContext';

export default function CubeProvider({ cubeApi, children, options = {} }) {
  return (
    <CubeContext.Provider value={{
      cubeApi,
      options
    }}
    >
      {children}
    </CubeContext.Provider>
  );
}
