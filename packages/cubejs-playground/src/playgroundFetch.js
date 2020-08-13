import { fetch } from 'whatwg-fetch';

const playgroundFetch = (url, options) => {
  // eslint-disable-next-line prefer-const
  let { retries, ...restOptions } = options || {};
  retries = retries || 0;
  return fetch(url, restOptions)
    .then(async (r) => {
      if (r.status === 500) {
        let errorText = await r.text();
        try {
          const json = JSON.parse(errorText);
          errorText = json.error;
        } catch (e) {
          // Nothing
        }
        throw errorText;
      }
      return r;
    })
    .catch((e) => {
      if (e.message === 'Network request failed' && retries > 0) {
        return playgroundFetch(url, { options, retries: retries - 1 });
      }
      throw e;
    });
};

export default playgroundFetch;
