module.exports = {
  extendContext: (req) => {
    const requestValues = {
      token: req.headers.authorization,
      host: req.headers.host,
      path: req.url.split('?')[0]
    };

    // console.log(requestValues);

    return requestValues;
  },
};
