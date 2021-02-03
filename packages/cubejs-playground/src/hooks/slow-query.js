import { useContext } from 'react';

import { AppContext } from './index';

export default function useSlowQuery() {
  const { slowQuery } = useContext(AppContext);

  return slowQuery;
}
