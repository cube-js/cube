import cookie from "component-cookie";
import uuidv4 from "uuid/v4";
import { fetch } from "whatwg-fetch";

let URL;
if (process.env.NODE_ENV === 'production') {
  URL = window.location.origin
} else {
  URL = "http://localhost:4000"
}

const COOKIE_NAME = "real_time_dashboard_uid";
const track = (eventName) => {
  if (!cookie(COOKIE_NAME)) {
    cookie(COOKIE_NAME, uuidv4());
  }

  fetch(`${URL}/collect`, {
    method: "POST",
    body: JSON.stringify({
      anonymousId: cookie(COOKIE_NAME),
      eventType: eventName
    }),
    headers: {
      "Content-Type": "application/json"
    }
  });
};

export default {
  pageview: () => track("pageView"),
  event: (eventName) => track(eventName)
}
