/**
 * @title @cubejs-client/react
 * @permalink /@cubejs-client-react
 * @category Cube.js Frontend
 * @subcategory Reference
 * @menuOrder 3
 * @description `@cubejs-client/react` provides React Components for easy Cube.js integration in a React app.
 */

declare module '@cubejs-client/react' {
  export default class App extends React.Component<AppState, AppProps> {
    render(): JSX.Element;
  }
}
