import React, { useEffect } from 'react';
import CubeContext from './CubeContext';

export default function CubeProvider({ cubeApi, cubejsApi, children, options = {} }) {
  useEffect(() => {
    if (cubejsApi && !cubeApi) {
      console.warn('"cubejsApi" is deprecated and will be removed in the following version. Use "cubeApi" instead.');
    }
  }, [cubeApi, cubejsApi]);
  
  return (
    <CubeContext.Provider value={{
      cubejsApi,
      cubeApi,
      options
    }}
    >
      {children}
    </CubeContext.Provider>
  );
}
