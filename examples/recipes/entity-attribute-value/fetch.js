const fetch = require('node-fetch');

const statusesQuery = {
  dimensions: [
    "Orders.status"
  ]
};

exports.fetchStatuses = async (host, path, token) => {
  const encodedQuery = encodeURIComponent(JSON.stringify(statusesQuery));
  const cubePath = `http://${host}${path}?query=${encodedQuery}`;
  
  const request = fetch(cubePath, {
    headers: {
      'Authorization': token
    }
  })

  // Works, because Cube runs this request asynchronously
  request
    .then(data => data.json())
    .then(json => json.data.map(entry => entry['Orders.status']))
    .then(statuses => console.log(statuses))

  // Doesn't work, because Cube is blocked by the original request being executed
  // const statuses = await request
  //   .then(data => data.json())
  //   .then(json => json.data.map(entry => entry['Orders.status']))

  // return statuses;
  
  return [ 'completed', 'processing', 'shipped' ];
}