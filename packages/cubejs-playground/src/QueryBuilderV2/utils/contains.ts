export function contains(
  string: string,
  substring: string,
  compare: (a: string, b: string) => number
): boolean {
  if (substring.length === 0) {
    return true;
  }

  string = string.normalize('NFC');
  substring = substring.normalize('NFC');

  let scan = 0;
  let sliceLength = substring.length;

  for (; scan + sliceLength <= string.length; scan++) {
    const sliced = string.slice(scan, scan + sliceLength);

    if (compare(substring, sliced) === 0) {
      return true;
    }
  }

  return false;
}
