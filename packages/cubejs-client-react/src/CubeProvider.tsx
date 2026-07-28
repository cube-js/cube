import React from 'react';

import CubeContext from './CubeContext';
import type { CubeContextProps, CubeProviderProps } from './types';

/**
 * Cube.js context provider
 * ```js
 * import React from 'react';
 * import cube from '@cubejs-client/core';
 * import { CubeProvider } from '@cubejs-client/react';
 *
 * const API_URL = 'https://harsh-eel.aws-us-east-2.cubecloudapp.dev';
 * const CUBE_TOKEN =
 *   'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.* eyJpYXQiOjE1OTE3MDcxNDgsImV4cCI6MTU5NDI5OTE0OH0.* n5jGLQJ14igg6_Hri_Autx9qOIzVqp4oYxmX27V-4T4';
 *
 * const cubeApi = cube(CUBE_TOKEN, {
 *   apiUrl: `${API_URL}/cubejs-api/v1`,
 * });
 *
 * export default function App() {
 *   return (
 *     <CubeProvider cubeApi={cubeApi}>
 *       //...
 *     </CubeProvider>
 *   )
 * }
 * ```
 * @stickyTypes
 * @order 10
 */
const CubeProvider: React.FC<CubeProviderProps> = ({ cubeApi, children, options = {} }) => (
  <CubeContext.Provider value={{
    cubeApi,
    options
  } as CubeContextProps}
  >
    {children}
  </CubeContext.Provider>
);

export default CubeProvider;
