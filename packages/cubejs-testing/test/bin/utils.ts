import fetch from 'node-fetch';

const DATASET_VERSION = 'v0.0.4';

export type DataSetSchema = {
  name: string,
  files: string[]
};

export function datasetUrl(file: string): string {
  return `https://raw.githubusercontent.com/cube-js/testing-fixtures/${DATASET_VERSION}/${file}`;
}

/**
 * GitHub's raw file CDN intermittently answers 502/504, see the "Gateway Time-out" failures
 * of `dataset:minimal` on CI.
 */
export async function withRetries<T>(label: string, fn: () => Promise<T>, attempts: number = 5): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (e: any) {
      // A 4xx (e.g. a file missing at DATASET_VERSION) will not go away, except a 429 rate limit;
      // network errors carry no status.
      if (attempt >= attempts || (e.status !== undefined && e.status < 500 && e.status !== 429)) {
        throw e;
      }

      const delay = 2 ** attempt * 1000;
      console.log(`[dataset] ${label} failed (attempt ${attempt}/${attempts}): ${e.message}, retrying in ${delay / 1000}s`);
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

export async function getDataSetDescription(name: string): Promise<DataSetSchema> {
  const url = datasetUrl('dataset.json');
  const dataSets: DataSetSchema[] = await withRetries('dataset.json', async () => {
    const response = await fetch(url);
    if (!response.ok) {
      throw Object.assign(
        new Error(`unexpected response ${response.status} ${response.statusText} (${url})`),
        { status: response.status },
      );
    }

    return response.json();
  });

  const dataSet = dataSets.find((ds) => ds.name === name);
  if (dataSet) {
    return dataSet;
  }

  throw new Error(
    `Unable to find data set with name: ${name}`
  );
}
