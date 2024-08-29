import { Meta, Query } from '@cubejs-client/core';

import { CubeGraphQLConverter } from '../../components/GraphQL/CubeGraphQLConverter';
import { MemberTypeMap } from '../../utils';

const API_SUFFIX = '/graphql-to-json';

export async function convertGraphQLToJsonQuery({
  query,
  apiUrl,
  apiToken,
}: {
  query: string;
  apiUrl: string;
  apiToken?: string | null;
}) {
  if (!query.trim() || !apiUrl) {
    return '{}';
  }

  const r = await fetch(apiUrl + API_SUFFIX, {
    method: 'POST',
    headers: {
      Authorization: apiToken || '',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query }),
  });

  const json = await r.json();

  return json.jsonQuery ? JSON.stringify(json.jsonQuery, null, 2) : '{}';
}

function metaToTypes(meta: Meta) {
  const types: MemberTypeMap = {};

  Object.values(meta.cubesMap).forEach((membersByType) => {
    Object.values(membersByType).forEach((members) => {
      Object.values<any>(members).forEach(({ name, type }) => {
        types[name] = type;
      });
    });
  });

  return types;
}

export function convertJsonQueryToGraphQL({
  meta,
  query,
}: {
  meta?: Meta | null;
  query: Query;
}) {
  const types = meta ? metaToTypes(meta) : null;

  if (!types) {
    return '';
  }

  try {
    const converter = new CubeGraphQLConverter(query, types);

    return converter.convert();
  } catch (error) {
    return `# ${error}\n`;
  }
}
