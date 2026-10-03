import { getContext, setContext } from 'svelte';

import type { CubeContextValue } from './types';

const CUBE_CONTEXT = Symbol('@cubejs-client/svelte');

export function setCubeContext(value: CubeContextValue): CubeContextValue {
  setContext(CUBE_CONTEXT, value);
  return value;
}
export function getCubeContext(): CubeContextValue;
export function getCubeContext(options: { optional: true }): CubeContextValue | null;
export function getCubeContext(
  options: { optional?: boolean } = {}
): CubeContextValue | null {
  const value = getContext<CubeContextValue | undefined>(CUBE_CONTEXT);

  if (!value && !options.optional) {
    throw new Error(
      'Cube context is not available. Wrap the component in <CubeProvider> or pass cubeApi directly.'
    );
  }

  return value ?? null;
}

export function tryGetCubeContext(): CubeContextValue | null {
  try {
    return getCubeContext({ optional: true });
  } catch {
    return null;
  }
}
