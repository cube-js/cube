import React, { useState } from "react";
import { withRouter } from "react-router";
import { Layout, Button } from "antd";
import tracker from "../tracker";

const Header = ({ location }) => {
  const [sendingEvent, setSendingEvent] = useState(false);
  return (
    <Layout.Header
      style={{
        padding: "0 32px"
      }}
    >
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
      </div>
    </Layout.Header>
  )
};

export default withRouter(Header);
