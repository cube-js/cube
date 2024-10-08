const underbar = new RegExp('_', 'g');
const dot = new RegExp('\\.', 'g');
const nonTitlecasedWords = [
  'and',
  'or',
  'nor',
  'a',
  'an',
  'the',
  'so',
  'but',
  'to',
  'of',
  'at',
  'by',
  'from',
  'into',
  'on',
  'onto',
  'off',
  'out',
  'in',
  'over',
  'with',
  'for',
];

function capitalize(str: string) {
  str = str.toLowerCase();

  return str.substring(0, 1).toUpperCase() + str.substring(1);
}

export function titleize(str: string) {
  str = str.toLowerCase().replace(underbar, ' ').replace(dot, ' ');
  const strArr = str.split(' ');
  const j = strArr.length;
  let d: string[], l: number;

  for (let i = 0; i < j; i++) {
    d = strArr[i].split('-');
    l = d.length;

    for (let k = 0; k < l; k++) {
      if (nonTitlecasedWords.indexOf(d[k].toLowerCase()) < 0) {
        d[k] = capitalize(d[k]);
      }
    }

    strArr[i] = d.join('-');
  }

  str = strArr.join(' ');
  str = str.substring(0, 1).toUpperCase() + str.substring(1);

  return str;
}
