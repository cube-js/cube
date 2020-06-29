export function heading(level: number) {
  return [...Array(level)].map(() => '#').join('');
}
