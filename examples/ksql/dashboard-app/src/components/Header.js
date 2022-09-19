import React, { useState } from "react";
import { Layout, Button } from "antd";
import { useCallback } from "react";

const Header = ({ analytics }) => {
  const [sendingEvent, setSendingEvent] = useState(false);

  const onClick = useCallback(() => {
    setSendingEvent(true);
    analytics.track("button_clicked");
    setTimeout(() => setSendingEvent(false), 2500);
  }, [analytics]);

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
