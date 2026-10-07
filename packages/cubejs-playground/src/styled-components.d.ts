// The bare import makes this a module augmentation; without it `declare module`
// shadows styled-components' own types instead of merging into them.
import 'styled-components';

declare module 'styled-components' {
  export interface DefaultTheme {
    background: string;
  }
}
