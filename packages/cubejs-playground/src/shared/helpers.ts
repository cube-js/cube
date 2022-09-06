import { AvailableCube, AvailableMembers } from '@cubejs-client/react';
import { MemberType, BaseCubeMember } from '@cubejs-client/core';
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

export function notEmpty<T>(value: T | null | undefined): value is T {
  return value != null;
}

export function ucfirst(s: string): string {
  return s[0].toUpperCase() + s.slice(1);
}

export function getNameMemberPairs(members: AvailableCube[]) {
  const items: [memberName: string, member: BaseCubeMember & MemberType][] = [];

  members.forEach((cube) =>
    cube.members.forEach((member) => {
      items.push([member.name, member]);
    })
  );

  return items;
}

export type MembersByCube = {
  cubeName: string;
  cubeTitle: string;
  measures: BaseCubeMember[];
  dimensions: BaseCubeMember[];
  segments: BaseCubeMember[];
  timeDimensions: BaseCubeMember[];
};

export function getMembersByCube(
  availableMembers: AvailableMembers
): MembersByCube[] {
  const membersByCube: Record<string, MembersByCube> = {};

  Object.entries(availableMembers).forEach(([memberType, cubes]) => {
    cubes.forEach((cube) => {
      if (!membersByCube[cube.cubeName]) {
        membersByCube[cube.cubeName] = {
          cubeName: cube.cubeName,
          cubeTitle: cube.cubeTitle,
          measures: [],
          dimensions: [],
          segments: [],
          timeDimensions: [],
        };
      }

      cube.members.forEach((member) => {
        membersByCube[cube.cubeName] = {
          ...membersByCube[cube.cubeName],
          [memberType]: [...membersByCube[cube.cubeName][memberType], member],
        };
      });
    });
  });

  return Object.values(membersByCube);
}

export function playgroundFetch(url, options: any = {}) {
  const { retries = 0, ...restOptions } = options;

  return fetch(url, restOptions)
    .then(async (r) => {
      if (r.status === 500) {
        let errorText = await r.text();
        try {
          const json = JSON.parse(errorText);
          errorText = json.error;
        } catch (e) {
          // Nothing
        }
        throw errorText;
      }
      return r;
    })
    .catch((e) => {
      if (e.message === 'Network request failed' && retries > 0) {
        return playgroundFetch(url, { options, retries: retries - 1 });
      }
      throw e;
    });
}

type RequestOptions = {
  token?: string;
  body?: Record<string, any>;
  headers?: Record<string, string>;
};

export async function request(
  endpoint: string,
  method: string = 'GET',
  options: RequestOptions = {}
) {
  const { body, token } = options;

  const headers: Record<string, string> = {};

  if (token) {
    headers.authorization = token;
  }

  const response = await fetch(endpoint, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
    ...(body ? { body: JSON.stringify(body) } : null),
  });

  return {
    ok: response.ok,
    json: await response.json(),
  };
}

type OpenWindowOptions = {
  url: string;
  width?: number;
  height?: number;
  title?: string;
};

export function openWindow({
  url,
  title = '',
  width = 640,
  height = 720,
}: OpenWindowOptions) {
  const dualScreenLeft =
    window.screenLeft !== undefined ? window.screenLeft : window.screenX;
  const dualScreenTop =
    window.screenTop !== undefined ? window.screenTop : window.screenY;

  const w = window.innerWidth
    ? window.innerWidth
    : document.documentElement.clientWidth
    ? document.documentElement.clientWidth
    : screen.width;
  const h = window.innerHeight
    ? window.innerHeight
    : document.documentElement.clientHeight
    ? document.documentElement.clientHeight
    : screen.height;

  const systemZoom = w / window.screen.availWidth;
  const left = (w - width) / 2 / systemZoom + dualScreenLeft;
  const top = (h - height) / 2 / systemZoom + dualScreenTop;

  const newWindow = window.open(
    url,
    title,
    `
      scrollbars=yes,
      width=${width / systemZoom}, 
      height=${height / systemZoom}, 
      top=${top}, 
      left=${left}
    `
  );

  newWindow?.focus?.();

  return newWindow;
}
