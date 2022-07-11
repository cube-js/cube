import { useContext } from 'react';
import CubeContext from '../CubeContext';

export function useCubeApi(explicitApiOrName) {
  const context = useContext(CubeContext);
  if (typeof explicitApiOrName === 'string') {
    return context?.[explicitApiOrName];
  } else {
    return explicitApiOrName || context?.default;
  }
}
