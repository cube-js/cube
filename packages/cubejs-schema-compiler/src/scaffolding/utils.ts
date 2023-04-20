export function toSnakeCase(str: string) {
  const match = str.match(/[A-Z]{2,}(?=[A-Z][a-z]+[0-9]*|\b)|[A-Z]?[a-z]+[0-9]*|[A-Z]|[0-9]+/g);

  if (!match) {
    return str;
  }

  return match.map(v => v.toLowerCase())
    .join('_');
}
