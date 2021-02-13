import cubejs from '@cubejs-client/core';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:4000/cubejs-api/v1';

export default (accessToken) => {
 return cubejs({
  apiUrl: `${API_URL}`,
  headers: {
    Authorization: `Bearer ${accessToken}`
  },
 });
};
