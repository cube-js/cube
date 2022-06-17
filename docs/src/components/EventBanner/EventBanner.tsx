import React, { useEffect, useState } from "react";
import { loadEvents, Event } from "./lib/api";
import { setUTM } from "./lib/link";
import { checkEvents, readEvents, writeEvents } from "./lib/browser";

import * as styles from './styles.module.scss';

function EventBanner () {
  const [isLoading, setLoading] = useState(true);
  const [events, setEvents] = useState<Event[]>([]);

  useEffect(() => {
    if (checkEvents()) {
      const events = readEvents();
      setEvents(events);
      setLoading(false);
    } else {
      loadEvents()
        .then((events) => {
          writeEvents(events);
          setEvents(events);
          setLoading(false);
        });
    }
  }, []);

  return (
    <div className={`${styles.banner} ${!isLoading ? styles.visible : ''}`}>
      {!isLoading
        && events.map(({id, link, message, campaign}) => (
          <a
            key={id}
            className={styles.banner__link}
            href={setUTM(
              link,
              'docs',
              campaign === null ? undefined : campaign
            )}
            target="_blank"
            rel="noreferrer"
          >{message}</a>
        ))
      }
    </div>
  );
}

export default EventBanner;
