import { useLocalStorage } from './local-storage';

type ListMode = 'bi' | 'dev';

export function useListMode(defaultMode?: ListMode): [ListMode, (listMode: ListMode) => void] {
  let [listMode, setListMode] = useLocalStorage<ListMode>('QueryBuilder:ForceListMode', 'bi');

  if (!['bi', 'dev'].includes(listMode)) {
    listMode = defaultMode ?? 'bi';
  }

  return [listMode, setListMode];
}
