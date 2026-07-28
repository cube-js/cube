import React from 'react';

import CubeContext from './CubeContext';
import type { CubeContextProps, CubeProviderProps } from './types';

export default function CubeProvider({ cubeApi, children, options = {} }: CubeProviderProps) {
  return (
    <CubeContext.Provider value={{
      cubeApi,
      options
    } as CubeContextProps}
    >
      {children}
    </CubeContext.Provider>
  );
}
