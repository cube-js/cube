import cookie from "component-cookie";
import uuidv4 from "uuid/v4";
import { fetch } from "whatwg-fetch";

const COOKIE_NAME = "real_time_dashboard_uid";

const track = async (eventName) => {
  if (!cookie(COOKIE_NAME)) {
    cookie(COOKIE_NAME, uuidv4());
  }

  return fetch(process.env.REACT_APP_COLLECT_URL, {
    method: 'POST',
    body: JSON.stringify({
      anonymousId: cookie(COOKIE_NAME),
      eventType: eventName
    }),
    headers: {
      'Content-Type': 'application/json'
    }
  });
};

export default {
  pageview: () => track('pageView'),
  event: (eventName) => track(eventName)
}
