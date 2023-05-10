import Anser from 'anser';
import { encode } from 'html-entities';

const colors = {
  reset: 'orange',
  black: 'black',
  red: 'red',
  green: 'green',
  yellow: 'orange',
  blue: 'blue',
  magenta: 'magenta',
  cyan: 'cyan',
  gray: 'gray',
  lightgrey: 'lightgrey',
  darkgrey: 'darkgrey',
};

const anserMap = {
  'ansi-bright-black': 'black',
  'ansi-bright-yellow': 'yellow',
  'ansi-yellow': 'yellow',
  'ansi-bright-green': 'green',
  'ansi-green': 'green',
  'ansi-bright-cyan': 'cyan',
  'ansi-cyan': 'cyan',
  'ansi-bright-red': 'red',
  'ansi-red': 'red',
  'ansi-bright-magenta': 'magenta',
  'ansi-magenta': 'magenta',
  'ansi-white': 'darkgrey',
};

export function generateAnsiHTML(txt: string) {
  const arr = new Anser().ansiToJson(encode(txt), {
    use_classes: true,
  });

  let result = '';
  let open = false;
  for (let index = 0; index < arr.length; ++index) {
    const c = arr[index];
    const { content } = c;
    const { fg } = c;

    const contentParts = content.split('\n');
    for (let j = 0; j < contentParts.length; ++j) {
      if (!open) {
        result += '<span data-ansi-line="true">';
        open = true;
      }
      const part = contentParts[j].replace('\r', '');
      const color = colors[anserMap[fg]];

      if (color != null) {
        result += `<span style="color: ${color};">${part}</span>`;
      } else {
        if (fg != null) {
          console.log('Missing color mapping: ', fg);
        }

        result += `<span>${part}</span>`;
      }

      if (j < contentParts.length - 1) {
        result += '</span>';
        open = false;
        result += '<br/>';
      }
    }
  }

  if (open) {
    result += '</span>';
  }

  return result;
}