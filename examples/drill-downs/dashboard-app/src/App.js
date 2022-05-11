import React from "react";
import "./App.css";
import 'fontsource-roboto';
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const AppLayout = ({ children }) => {
  return (
    <div className="app">
      <div>{children}</div>
    </div>
  );
};

const App = ({ children }) => (
  <CubeProvider cubejsApi={cubejsApi}>
    <AppLayout>{children}</AppLayout>
  </CubeProvider>
);

export default App;
