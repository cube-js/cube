import cubejs, { HttpTransport } from '@cubejs-client/core';
 
const API_URL = 'http://localhost:4000';
 
export default async (accessToken) => {
 return cubejs({
   transport: new HttpTransport({
     authorization: `Bearer ${accessToken}`,
     apiUrl: `${API_URL}/cubejs-api/v1`,
     headers: { 'custom-header': 'value' },
   }),
 });
};
