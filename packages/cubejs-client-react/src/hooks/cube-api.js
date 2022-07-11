import { useContext } from 'react';
import CubeContext from '../CubeContext';

export function useCubeApi(explicitApi) {
  const context = useContext(CubeContext);
  return explicitApi || context?.cubejsApi;
}
