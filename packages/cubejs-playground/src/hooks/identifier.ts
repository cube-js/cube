import { useAppContext } from '../components/AppContext';

export function useIdentifier(): string {
  const { identifier } = useAppContext();

  return identifier || '';
}
