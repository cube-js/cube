import { useContext } from 'react';

import { AppContext } from './index';

export default function useIsPreAggregationBuildInProgress() {
  const { isPreAggregationBuildInProgress } = useContext(AppContext);

  return isPreAggregationBuildInProgress;
}
