export function stripLineBreaks(this: string) {
  return this.replace(/\n/g, ' ');
}
