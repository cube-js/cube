import fetch from 'node-fetch';

const DATASET_VERSION = 'v0.0.4';

export type DataSetSchema = {
  name: string,
  files: string[]
};

export async function getDataSetDescription(name: string): Promise<DataSetSchema> {
  const response = await fetch(
    `https://raw.githubusercontent.com/cube-js/testing-fixtures/${DATASET_VERSION}/dataset.json`
  );

  const dataSets: DataSetSchema[] = await response.json();

  const dataSet = dataSets.find((ds) => ds.name === name);
  if (dataSet) {
    return dataSet;
  }

  throw new Error(
    `Unable to find data set with name: ${name}`
  );
}
