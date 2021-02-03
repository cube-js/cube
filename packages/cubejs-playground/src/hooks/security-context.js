import { useContext } from 'react';
import { SecurityContextContext } from '../components/SecurityContext/SecurityContext';

export default function useSecurityContext() {
  return useContext(SecurityContextContext)
}
