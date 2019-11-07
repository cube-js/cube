import React, { useState } from "react";
import { Link } from "react-router-dom";
import { withRouter } from "react-router";
import { Layout, Button, Divider } from "antd";
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
          <img src={cubejsLogo} height={40} />
          <p className="stats">Real Time Demo</p>
        </h2>
      </div>
      <div style={{display: "flex", justifyContent: "flex-end", height: "100%", alignItems: "center"}}>
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
        </div>
    </Layout.Header>
  )
};

export default withRouter(Header);
