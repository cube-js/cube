module.exports = (req, res) => {
  const path = req.originalUrl || req.path || req.url;
  const httpHeader = req.header && req.header('x-forwarded-for');
  const ip = req.ip || httpHeader || req.connection.remoteAddress;
  const requestData = {
    path,
    method: req.method,
    status: res.statusCode,
    ip,
    time: (new Date()).toISOString(),
  };

  if (res.get) {
    requestData.contentLength = res.get('content-length');
    requestData.contentType = res.get('content-type');
  }

  return requestData;
};
