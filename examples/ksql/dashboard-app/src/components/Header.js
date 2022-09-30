import React, { useState } from "react";
import { Layout, Button } from "antd";
import { useCallback } from "react";

const Header = ({ analytics }) => {
  const [sendingEvent, setSendingEvent] = useState(false);

  const onClick = useCallback(() => {
    setSendingEvent(true);
    analytics.track("button_clicked");
    setTimeout(() => setSendingEvent(false), 100);
  }, [analytics]);

  return (
    <Layout.Header style={{ padding: "0 32px" }}>
      <div className="top-menu">
        <Button
          onClick={onClick}
          type="primary"
        >Click Me Now</Button>
      </div>
    </Layout.Header>
  )
};

export default Header;
