export function getRealType(value: any): string {
  if (value === null) {
    return 'null';
  }

  return typeof value;
}
