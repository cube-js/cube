import React, { useState } from "react";
import { withRouter } from "react-router";
import { Layout, Button, Divider, Icon } from "antd";
import cubejsLogo from "../cubejs-logo.png";
import tracker from "../tracker";

const Header = ({ location }) => {
  const [sendingEvent, setSendingEvent] = useState(false);
  return (
    <Layout.Header
      style={{
        padding: "0 32px"
      }}
    >
      <div
        style={{
          float: "left"
        }}
      >
        <h2
          style={{
            color: "#fff",
            margin: 0,
            marginRight: "1em"
          }}
        >
          <img alt="cubejs-logo" src={cubejsLogo} height={40} />
          <p className="stats">Real Time Demo</p>
        </h2>
      </div>
      <div className="top-menu">
        <Button
          onClick={() => {
            setSendingEvent(true);
            setTimeout(() => setSendingEvent(false), 2500);
            tracker.event("buttonClicked");
          }}
          loading={sendingEvent}
          type="primary"
        >
          {sendingEvent
            ? "Sending Button Click Event"
            : "Send Button Click Event"}
        </Button>
        <Divider type="vertical" />
        <a href="https://github.com/cube-js/cube.js/tree/master/examples/real-time-dashboard">
          <Icon type="github" />
          <span>Github</span>
        </a>
        <a href="https://slack.cube.dev">
          <Icon type="slack" />
          <span>Slack</span>
        </a>
      </div>
    </Layout.Header>
  )
};

export default withRouter(Header);
