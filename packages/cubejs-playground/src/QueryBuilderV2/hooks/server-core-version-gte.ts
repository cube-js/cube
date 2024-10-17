export function useServerCoreVersionGte(version: string, currentVersion: string): boolean {
  if (currentVersion) {
    let gt = false;

    try {
      const [major, minor, patch] = currentVersion.split('.').map(Number);
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
