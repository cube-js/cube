import { useAppContext } from '../components/AppContext';

export function useServerCoreVersionGt(version: string): boolean {
  const {
    playgroundContext: { serverCoreVersion = '', coreServerVersion = '' } = {},
  } = useAppContext();

  let gt = false;

  try {
    const [, m, p] = (serverCoreVersion || coreServerVersion).split('.').map(Number);
    const [, m1, p1] = version.split('.').map(Number);

    gt = m > m1 || (m === m1 && p >= p1);
  } catch (_) {
    //
  }

  return gt;
}
