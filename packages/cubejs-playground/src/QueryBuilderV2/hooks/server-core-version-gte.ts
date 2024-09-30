export function useServerCoreVersionGte(version: string, currentVersion: string): boolean {
  if (currentVersion) {
    let gt = false;

    try {
      const [, m, p] = currentVersion.split('.').map(Number);
      const [, m1, p1] = version.split('.').map(Number);

      gt = m > m1 || (m === m1 && p >= p1);
    } catch (_) {
      //
    }

    return gt;
  }

  return true;
}
