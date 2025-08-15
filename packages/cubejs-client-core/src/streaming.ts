export async function* responseChunks(res: Response): AsyncIterable<Uint8Array> {
  // eslint-disable-next-line prefer-destructuring
  const body: any = res.body;

  if (body && typeof body.getReader === 'function') {
    const reader = body.getReader(); // Browser / Node native fetch
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) yield value; // Uint8Array
      }
    } finally {
      reader.releaseLock?.();
    }
    return;
  }

  // Node.js Readable (node-fetch v2 via cross-fetch)
  if (body && Symbol.asyncIterator in body) {
    for await (const chunk of body as AsyncIterable<Buffer | Uint8Array | string>) {
      if (typeof chunk === 'string') {
        // Convert string chunks to bytes (rare, but safe)
        yield new TextEncoder().encode(chunk);
      } else {
        yield new Uint8Array(chunk);
      }
    }
    return;
  }

  throw new Error('Unsupported response body type for streaming');
}
