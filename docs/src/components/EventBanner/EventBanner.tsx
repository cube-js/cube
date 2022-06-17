import React, { useEffect, useState } from "react";
import { loadEvents, Event } from "./lib/api";
import { setUTM } from "./lib/link";
import * as styles from './styles.module.scss';

function EventBanner () {
  const [loading, setLoading] = useState(true);
  const [events, setEvents] = useState<Event[]>([]);

  useEffect(() => {
    loadEvents()
      .then((events) => {
        setEvents(events);
        setLoading(false);
      });
  }, []);

  return (
    <div className={`${styles.banner} ${!loading ? styles.visible : ''}`}>
      {!loading
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
