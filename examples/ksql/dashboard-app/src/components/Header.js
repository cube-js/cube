import React, { useState, useEffect } from "react";
import { Layout, Button } from "antd";
import { useCallback } from "react";

const DELAY = 1000;
const EVERY = 5;

const Header = ({ analytics }) => {
  const [sendingEvent, setSendingEvent] = useState(false);
  const [ i, setI ] = useState(0);

  const onClick = useCallback(() => {
    setSendingEvent(true);
    analytics.track("button_clicked");
    setTimeout(() => setSendingEvent(false), 500);
  }, [analytics]);

  useEffect(() => {
    const interval = setTimeout(() => {
      setI(i => i + 1);

      if (i % EVERY === 0) {
        onClick();
      }
    }, DELAY);
    return () => clearTimeout(interval);
  }, [i, onClick]);

  return (
    <Layout.Header style={{ padding: "0 32px" }}>
      <div className="top-menu">
        <span style={{ padding: '0.5em' }}>
          I will click myself in
          <span style={{ display: 'inline-block', width: '1em', textAlign: 'center' }}>{Math.ceil(i / EVERY) * EVERY - i + 1}</span>
          seconds or
        </span>
        <Button
          onClick={onClick}
          loading={sendingEvent}
          type="primary"
        >Click Me Now</Button>
      </div>
    </Layout.Header>
  )
};

export default Header;
