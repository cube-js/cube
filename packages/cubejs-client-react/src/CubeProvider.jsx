import React from 'react';
import CubeContext from './CubeContext';

export default function CubeProvider({ cubejsApi, children }) {
  return <CubeContext.Provider value={{ cubejsApi }}>{children}</CubeContext.Provider>;
}
