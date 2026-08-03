import { usePlaygroundContext } from './app-context';

export function useServerCoreVersionGte(version: string): boolean {
  const { serverCoreVersion = '', coreServerVersion = '' } =
    usePlaygroundContext();

  const coreVersion = serverCoreVersion || coreServerVersion;

  if (coreVersion) {
    let gt = false;

    try {
      const [major, minor, patch] = coreVersion.split('.').map(Number);
      const [major1, minor1, patch1] = version.split('.').map(Number);

      gt =
        major > major1 ||
        (major === major1 && minor > minor1) ||
        (major === major1 && minor === minor1 && patch >= patch1);
    } catch (_) {
      //
    }

    return gt;
  }

  return true;
}
