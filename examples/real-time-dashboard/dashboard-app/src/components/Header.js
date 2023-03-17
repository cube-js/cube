import React, { useState } from "react";
import { Layout, Button } from "antd";
import tracker from "../tracker";
import { useCallback } from "react";

const Header = () => {
  const [sendingEvent, setSendingEvent] = useState(false);

  const onClick = useCallback(() => {
    setSendingEvent(true);
    tracker.event("buttonClicked");
    setTimeout(() => setSendingEvent(false), 2500);
  }, []);

  return (
    <Layout.Header style={{ padding: "0 32px" }}>
      <div className="top-menu">
        <Button
          onClick={onClick}
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

export default Header;
