import { useAppContext } from './app-context';

export function useIdentifier(): string {
  const { identifier } = useAppContext();

  return identifier || '';
}
