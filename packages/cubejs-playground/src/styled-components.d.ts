// styled-components 6.5 stopped widening an unaugmented DefaultTheme to `any`, so every
// `props.theme` read has to be declared. CodeSnippet is the only ThemeProvider here.
import 'styled-components';

declare module 'styled-components' {
  export interface DefaultTheme {
    background: string;
  }
}
