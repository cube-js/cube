export const getUserPreference = (preference) => (
  JSON.parse(window.localStorage.getItem(preference))
);

export const setUserPreference = (key, preference) => window.localStorage.setItem(key, JSON.stringify(preference))
