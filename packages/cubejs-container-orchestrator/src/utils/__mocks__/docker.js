module.exports = {
  run: jest.fn(() => Promise.resolve(true)),
  container: {
    exists: jest.fn(() => Promise.resolve(false)),
    stop: jest.fn(() => Promise.resolve(true)),
  },
};
