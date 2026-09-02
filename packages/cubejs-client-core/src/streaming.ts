export async function* responseChunks(res: Response): AsyncIterable<Uint8Array> {
  if (!res.body) {
    throw new Error('Unsupported response body type for streaming');
  }

  const reader = res.body.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) yield value;
    }
  } finally {
    reader.releaseLock();
  }
}
