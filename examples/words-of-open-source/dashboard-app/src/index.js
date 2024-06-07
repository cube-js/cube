import createExampleWrapper from "cube-example-wrapper";

createExampleWrapper({
  title: "Words of Open Source",
  text: `
    <p>Ratio of the number of commits containing a certain word to the total number of commits.</p>
    <p>Based on the public dataset of <a href="https://console.cloud.google.com/marketplace/product/github/github-repos">GitHub Activity Data</a> and powered by <a href="https://cube.dev">Cube</a>.</p>
  `,
});

import ReactDOM from "react-dom";
import { App } from "./App";

const app = document.getElementById("app");
ReactDOM.render(<App />, app);