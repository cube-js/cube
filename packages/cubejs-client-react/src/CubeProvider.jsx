import React, { useContext, useMemo } from 'react';
import CubeContext from './CubeContext';

export default function CubeProvider({
  name,
  cubejsApi,
  children,
}) {
  const oldContext = useContext(CubeContext);
  const newContext = useMemo(() => ({
    ...oldContext,
    [name ?? 'default']: cubejsApi,
  }), []);

  return <CubeContext.Provider value={newContext}>{children}</CubeContext.Provider>;
}
