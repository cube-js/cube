import { useContext } from 'react';
import { AppContext } from '../App';

export default function useSlowQuery() {
  const { slowQuery } = useContext(AppContext);

  return slowQuery;
}
