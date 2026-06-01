export function parseApiSecretsEnv(raw: string | undefined): string[] | undefined {
  if (!raw) {
    return undefined;
  }
  const list = Array.from(
    new Set(raw.split(',').map((s) => s.trim()).filter(Boolean))
  );
  return list.length > 0 ? list : undefined;
}
