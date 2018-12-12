import { fetch } from 'whatwg-fetch';
import cookie from 'component-cookie';
import uuidv4 from 'uuid/v4';

export const trackPageView = () => {
  if (!cookie('aws_web_uid')) {
    cookie('aws_web_uid', uuidv4());
  }
  fetch(
    'https://4bfydqnx8i.execute-api.us-east-1.amazonaws.com/dev/collect',
    {
      method: 'POST',
      body: JSON.stringify({
        url: window.location.href,
        referrer: document.referrer,
        anonymousId: cookie('aws_web_uid'),
        eventType: 'pageView'
      }),
      headers: {
        'Content-Type': 'application/json'
      }
    }
  )
}
