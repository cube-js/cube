import React, { useEffect, useState } from 'react';
import cubejs, {CubejsApi} from '@cubejs-client/core';

const CubeQueryResultSet = (props: propsType) => {
  const { api, token, query } = props;
  const [code, setCode] = useState('');
  const defaultToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2Mjc0NjM2MDZ9.1boj2JrVcsxVkbQsZxuOP21VDxNQyHpxrh3go45k9pc";

  const cubejsApi = cubejs(
  token || defaultToken,
  { apiUrl: api }
  );

  useEffect(() => {
    fetchDataFromCube(cubejsApi,query,setCode);
  }, []);

  return (
    <div>
      <pre>
        <code className={`language-javascript`}>{code}</code>
      </pre>
    </div>
  );
};

export default CubeQueryResultSet;

async function fetchDataFromCube(
  cubejsApi: CubejsApi,
  query: object,
  setCode: (text: string) => void,
) {
  try {
    const resultSet = await cubejsApi.load(query);
    const result = resultSet?.rawData()?.[0]
    if (result) {
      setCode(JSON.stringify(result, null, 2));
    }
    highlightCode();
  } catch (e) {
    console.log(e);
  }
}

function highlightCode(): void {
  window.Prism && window.Prism.highlightAll();
}

interface propsType {
  api: string;
  token: string;
  query: object;
}
