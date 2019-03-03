import cubejs from '@cubejs-client/core';

export default cubejs(process.env.REACT_APP_CUBEJS_API_KEY,
  { apiUrl: process.env.REACT_APP_CUBEJS_API_URL }
);
