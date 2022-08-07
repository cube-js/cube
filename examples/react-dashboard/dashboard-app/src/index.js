import React from "react";
import { createRoot } from "react-dom/client";
import "./index.less";
import App from "./App";
import * as serviceWorker from "./serviceWorker";
import { Route, Routes, BrowserRouter } from "react-router-dom";
import createExampleWrapper from "@cube-dev/example-wrapper";
// import ExplorePage from "./pages/ExplorePage";
// import DashboardPage from "./pages/DashboardPage";

createExampleWrapper({ title: "React Dashboard" });

const root = createRoot(
  document.getElementById("root")
);

root.render(
  <BrowserRouter>
      <App />
  </BrowserRouter>
); // If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA

// root.render(
//   <Router>
//     <App>
//       <Route key="index" exact path="/" component={DashboardPage} />
//       <Route key="explore" path="/explore" component={ExplorePage} />
//     </App>
//   </Router>
//   );

/* <Router>
<App>
  <Route key="index" exact path="/" component={DashboardPage} />
  <Route key="explore" path="/explore" component={ExplorePage} />
</App>
</Router> */

serviceWorker.unregister();
