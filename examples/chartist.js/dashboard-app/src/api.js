import { CubejsApi } from '@cubejs-client/core';

const apiUrl = 'https://heavy-lansford.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1';
const cubeToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.OHZOpOBVKr-sCwn8sbZ5UFsqI3uCs6e4omT7P6WVMFw';

const cubeApi = new CubejsApi(cubeToken, { apiUrl });

export async function getAquisitions() {
  const acquisitionsByYearQuery = {
    dimensions: [
      'Artworks.yearAcquired',
    ],
    measures: [
      'Artworks.count'
    ],
    filters: [{
      member: 'Artworks.yearAcquired',
      operator: 'set'
    }],
    order: {
      'Artworks.yearAcquired': 'asc'
    }
  };

  const resultSet = await cubeApi.load(acquisitionsByYearQuery);
  const amountByYear = resultSet.tablePivot().map(row => (parseInt(row['Artworks.count'])));
  const years = resultSet.tablePivot().map(row => (parseInt(row['Artworks.yearAcquired'])));

  return {
    amountByYear,
    years
  };
}

export async function getDimensions() {
  const dimensionsQuery = {
    dimensions: [
      'Artworks.widthCm',
      'Artworks.heightCm'
    ],
    filters: [
      {
        member: 'Artworks.classification',
        operator: 'equals',
        values: ['Painting']
      },
      {
        member: 'Artworks.widthCm',
        operator: 'set'
      },
      {
        member: 'Artworks.widthCm',
        operator: 'lt',
        values: ['500']
      },
      {
        member: 'Artworks.heightCm',
        operator: 'set'
      },
      {
        member: 'Artworks.heightCm',
        operator: 'lt',
        values: ['500']
      }
    ]
  };

  const resultSet = await cubeApi.load(dimensionsQuery);

  return resultSet.tablePivot().map(row => ({
    x: parseInt(row['Artworks.widthCm']),
    y: parseInt(row['Artworks.heightCm']),
  }));
}
