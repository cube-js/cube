import { useContext } from 'react';
import { LivePreviewContextContext } from '../components/LivePreviewContext/LivePreviewContextProvider';

export default function useLivePreviewContext() {
  return useContext(LivePreviewContextContext);
}
