export async function* responseChunks(res: Response): AsyncIterable<Uint8Array> {
  // eslint-disable-next-line prefer-destructuring
  const body: any = res.body;

  if (!body || typeof body.getReader !== 'function') {
    throw new Error('Unsupported response body type for streaming');
  }

  const reader = body.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) yield value; // Uint8Array
    }
  } finally {
    reader.releaseLock?.();
  }
}
