type RequestOptions = {
  token?: string;
  body?: Record<string, any>;
  headers?: Record<string, string>;
};

export async function request(
  endpoint: string,
  method: string = 'GET',
  options: RequestOptions = {}
) {
  const { body, token } = options;

  const headers: Record<string, string> = {};

  if (token) {
    headers.authorization = token;
  }

  const response = await fetch(endpoint, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
    ...(body ? { body: JSON.stringify(body) } : null),
  });

  return {
    ok: response.ok,
    json: await response.json(),
  };
}