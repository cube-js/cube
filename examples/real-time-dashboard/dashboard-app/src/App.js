import React, { useEffect } from "react";
import "./App.css";
import { Layout } from "antd";
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import Header from "./components/Header";
import WebSocketTransport from "@cubejs-client/ws-transport";
import tracker from "./tracker";
const CUBEJS_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1NzI1ODQzNjd9.8Po-ZchZlEJEtqwq5hU0aHDnsJrlkpKs1P2Cs5dbfBQ";
let WS_URL;
if (process.env.NODE_ENV === 'production') {
  WS_URL = "wss://cubejs-real-time-demo.herokuapp.com/"
} else {
  WS_URL = "ws://localhost:4000/"
}
const cubejsApi = cubejs({
  transport: new WebSocketTransport({
    authorization: CUBEJS_TOKEN,
    apiUrl: WS_URL
  })
});

const AppLayout = ({ children }) => (
  <Layout
    style={{
      height: "100%"
    }}
  >
    <Header />
    <Layout.Content>{children}</Layout.Content>
  </Layout>
);

const App = ({ children }) => {
  useEffect(() => tracker.pageview(), []);
  return (
    <CubeProvider cubejsApi={cubejsApi}>
      <AppLayout>{children}</AppLayout>
    </CubeProvider>
  );
};

export default App;
