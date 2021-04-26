import { useContext } from 'react';
import { LivePreviewContextContext } from '../components/LivePreviewContext/LivePreviewContextProvider';

export function useLivePreviewContext() {
  return useContext(LivePreviewContextContext);
}
