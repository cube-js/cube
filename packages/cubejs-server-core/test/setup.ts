// Break on unhandled promise rejection
// https://github.com/facebook/jest/issues/3251#issuecomment-299183885
if (typeof process.env.LISTENING_TO_UNHANDLED_REJECTION === 'undefined') {
  process.on('unhandledRejection', (unhandledRejectionWarning) => {
    throw unhandledRejectionWarning; // see stack trace for test at fault
  });

  // Avoid memory leak by adding too many listeners
  process.env.LISTENING_TO_UNHANDLED_REJECTION = 'yes';
}
