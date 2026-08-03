/**
 * Capitalize the first letter of a word.
 * @param word
 */
export function capitalize<T extends string>(
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  word: T extends `${infer U} ${infer V}` ? 'You can capitalize only single word' : T
) {
  return word.charAt(0).toUpperCase() + word.slice(1);
}
