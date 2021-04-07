import { useContext } from 'react';
import { SecurityContextContext } from '../components/SecurityContext/SecurityContextProvider';

export function useSecurityContext() {
  return useContext(SecurityContextContext);
}
