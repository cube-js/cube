import { DialogForm, LoadingIcon, Radio, Space, TextArea, useForm } from '@cube-dev/ui-kit';
import { Meta, Query } from '@cubejs-client/core';
import { ValidationRule } from '@cube-dev/ui-kit/types/shared';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { parse as BestEffortJsonParse } from 'best-effort-json-parser';

import { useQueryBuilderContext } from '../context';
import { useServerCoreVersionGte } from '../hooks';
import { convertGraphQLToJsonQuery, convertJsonQueryToGraphQL, validateQuery } from '../utils';

interface PasteQueryDialogFormProps {
  query?: Query;
  defaultType?: QueryType;
  apiVersion?: string;
  onDismiss?: () => void;
  onSubmit: (query: Query) => void;
}

const DEFAULT_GRAPHQL_QUERY = `query CubeQuery {
  cube
}`;

function validateJsonQuery(json: string) {
  try {
    return validateQuery(BestEffortJsonParse(json));
  } catch (e: any) {
    throw 'Invalid query';
  }
}

function getGraphQLValidator(apiUrl: string, apiToken: string | null) {
  return [
    {
      async validator(rule: ValidationRule, query: string) {
        return convertGraphQLToJsonQuery({
          apiUrl,
          apiToken,
          query: query,
        }).then(
          (json) => validateJsonQuery(json),
          () => {
            throw '';
          }
        );
      },
    },
  ];
}

function getJSONValidator(apiUrl: string, apiToken: string | null, meta?: Meta | null) {
  return [
    {
      async validator(rule: ValidationRule, query: string) {
        const originalQuery = JSON.stringify(BestEffortJsonParse(query));
        const graphQLQuery = convertJsonQueryToGraphQL({
          meta,
          query: BestEffortJsonParse(query),
        });

        return convertGraphQLToJsonQuery({
          apiUrl,
          apiToken,
          query: graphQLQuery,
        }).then(
          (json) => {
            return originalQuery === json;
          },
          (e) => {
            throw '';
          }
        );
      },
    },
  ];
}

const QUERY_VALIDATOR = {
  async validator(rule: ValidationRule, value: string) {
    if (!validateJsonQuery) {
      throw 'Invalid query';
    }
  },
};
const JSON_VALIDATOR = {
  async validator(rule: ValidationRule, value: string) {
    try {
      BestEffortJsonParse(value);
    } catch (e: any) {
      throw ''; // do not show any error message
    }
  },
};

type QueryType = 'json' | 'graphql';

async function pause(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function EditQueryDialogForm(props: PasteQueryDialogFormProps) {
  const [form] = useForm();
  const { onSubmit, onDismiss, defaultType = 'json', query, apiVersion } = props;
  const [type, setType] = useState<QueryType>(defaultType);
  const isGraphQLSupported = apiVersion ? useServerCoreVersionGte('0.35.23', apiVersion) : true;
  const isGraphQLSupportedV1 = apiVersion ? useServerCoreVersionGte('0.35.27', apiVersion) : true;
  const [isBlocked, setIsBlocked] = useState(false);

  let { apiUrl, apiToken, meta } = useQueryBuilderContext();

  if (!isGraphQLSupportedV1) {
    apiUrl = apiUrl.replace(/\/v1$/, '');
  }

  async function parseAndPrepareQuery(query: string, type: QueryType) {
    if (type === 'graphql') {
      return validateQuery(
        JSON.parse(await convertGraphQLToJsonQuery({ query, apiUrl, apiToken })) || {}
      );
    }

    return validateQuery(BestEffortJsonParse(query) || {});
  }

  const onJsonBlur = useCallback(async () => {
    const type = form.getFieldValue('type');

    await pause(100);

    // check if onblur was triggered by type switch, skip if so
    if (type !== 'json') {
      return;
    }

    const jsonQuery = form.getFieldValue('jsonQuery');
    let sanitizedQuery = {};
    try {
      sanitizedQuery = validateQuery(BestEffortJsonParse(jsonQuery));
    } catch (e) {
      // do nothing
    }

    const query = sanitizedQuery;

    form.setFieldValue('jsonQuery', JSON.stringify(query, null, 2));
  }, [meta]);

  const onGraphqlBlur = useCallback(async () => {
    await pause(100);

    const graphqlQuery = form.getFieldValue('graphqlQuery');
    const type = form.getFieldValue('type');

    // check if onblur was triggered by type switch, skip if so
    if (type !== 'graphql') {
      return;
    }

    setIsBlocked(true);

    return convertGraphQLToJsonQuery({ query: graphqlQuery, apiUrl, apiToken })
      .then((jsonQuery) => {
        const query = validateQuery(JSON.parse(jsonQuery) || {});
        const graphqlQuery = convertJsonQueryToGraphQL({ meta, query });

        form.setFieldValue('graphqlQuery', graphqlQuery);
      })
      .finally(() => {
        setIsBlocked(false);
      });
  }, [meta]);

  const defaultQueryValue =
    type === 'json'
      ? JSON.stringify(query || {}, null, 2)
      : meta && query
        ? convertJsonQueryToGraphQL({ meta, query })
        : '';

  const onTypeChange = useCallback((type) => {
    setType(type);
    const originalQuery = form.getFieldValue(type === 'json' ? 'graphqlQuery' : 'jsonQuery');
    setIsBlocked(true);

    void parseAndPrepareQuery(originalQuery, type === 'json' ? 'graphql' : 'json')
      .then((query) => {
        const value =
          type === 'json'
            ? JSON.stringify(query || {}, null, 2)
            : query
              ? convertJsonQueryToGraphQL({ meta, query })
              : '';

        form.setFieldValue(type === 'json' ? 'jsonQuery' : 'graphqlQuery', value);
      })
      .catch((e) => {
        form.setFieldValue(
          type === 'json' ? 'jsonQuery' : 'graphqlQuery',
          type === 'json' ? '{}' : DEFAULT_GRAPHQL_QUERY
        );

        return 'Unable to convert query';
      })
      .finally(() => {
        setIsBlocked(false);
      });
  }, []);

  useEffect(() => {
    form.setFieldValue(type === 'json' ? 'jsonQuery' : 'graphqlQuery', defaultQueryValue);
  }, [JSON.stringify(query)]);

  useEffect(() => {
    form.setFieldValue('type', defaultType);
  }, [defaultType]);

  const onSubmitLocal = useCallback(async ({ type }) => {
    await (type === 'json' ? onJsonBlur() : onGraphqlBlur());

    const query =
      type === 'json' ? form.getFieldValue('jsonQuery') : form.getFieldValue('graphqlQuery');

    await parseAndPrepareQuery(query, type).then((query) => onSubmit(query));
  }, []);

  const graphqlRules = useMemo(() => [getGraphQLValidator(apiUrl, apiToken)], [apiUrl, apiToken]);
  const jsonRules = useMemo(() => [JSON_VALIDATOR, QUERY_VALIDATOR], []);

  return (
    <DialogForm
      form={form}
      title="Apply query"
      size="L"
      submitProps={{
        label: 'Apply',
      }}
      onSubmit={onSubmitLocal}
      onDismiss={onDismiss}
    >
      {isGraphQLSupported ? (
        <Space gap="1x">
          <Radio.ButtonGroup
            name="type"
            isDisabled={isBlocked}
            aria-label="Type"
            orientation="horizontal"
            onChange={onTypeChange}
          >
            <Radio.Button value="json">JSON</Radio.Button>
            <Radio.Button value="graphql">GraphQL</Radio.Button>
          </Radio.ButtonGroup>
          {isBlocked ? <LoadingIcon /> : null}
        </Space>
      ) : undefined}

      <div>
        <TextArea
          name="jsonQuery"
          aria-label="JSON Query"
          rules={jsonRules}
          isDisabled={isBlocked}
          wrapperStyles={{ height: '30x', hide: type === 'graphql' }}
          inputStyles={{ font: 'monospace' }}
          onBlur={onJsonBlur}
        />

        <TextArea
          name="graphqlQuery"
          aria-label="GraphQL Query"
          rules={graphqlRules}
          isDisabled={isBlocked}
          wrapperStyles={{ height: '30x', hide: type === 'json' }}
          inputStyles={{ font: 'monospace' }}
          onBlur={onGraphqlBlur}
        />
      </div>
    </DialogForm>
  );
}
