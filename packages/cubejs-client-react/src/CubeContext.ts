import { createContext } from 'react';

import type { CubeContextProps } from './types';

// The context has no default value: `cubeApi` is only available under a
// `CubeProvider`, and consumers guard against a missing context.
export default createContext<CubeContextProps>(null as unknown as CubeContextProps);
