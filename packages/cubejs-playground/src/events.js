export const event = (name, params) => {
  // eslint-disable-next-line no-undef
  if (window.analytics) {
    // eslint-disable-next-line no-undef
    window.analytics.track(name, params);
  }
};

export const playgroundAction = (name, options) => {
  event('Playground Action', { name, ...options });
};

export const page = (name) => {
  // eslint-disable-next-line no-undef
  if (window.analytics) {
    // eslint-disable-next-line no-undef
    window.analytics.page(name);
  }
};