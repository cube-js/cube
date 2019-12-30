const Pool = jest.fn().mockImplementation(() => ({
  on: () => {},
}));

module.exports = {
  Pool,
};
