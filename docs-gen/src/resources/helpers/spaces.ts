export function spaces(length: number) {
  return `!spaces${[...Array(length)].map(() => ' ').join('')}`;
}
