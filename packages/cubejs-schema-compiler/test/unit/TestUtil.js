export function debugLog(...args) {
  if (process.env.DEBUG_LOG === 'true') {
    // eslint-disable-next-line
    console.log(...args);
  }
}
