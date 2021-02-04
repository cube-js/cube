import { useContext } from 'react';
import { SecurityContextContext } from '../components/SecurityContext/SecurityContextProvider';

export default function useSecurityContext() {
  return useContext(SecurityContextContext);
}
