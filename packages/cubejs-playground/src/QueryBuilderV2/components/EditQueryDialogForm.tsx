import { DialogForm, LoadingIcon, Radio, Space, TextArea, useForm, ValidationRule } from '@cube-dev/ui-kit';
import { Query } from '@cubejs-client/core';
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
  } catch {
    throw new Error('Invalid query');
  }
}

function getGraphQLValidator(apiUrl: string, apiToken: string | null) {
  return [
    {
      async validator(rule: ValidationRule, query: string) {
        return convertGraphQLToJsonQuery({
          apiUrl,
          apiToken,
          query,
        }).then(
          (json) => validateJsonQuery(json),
          () => {
            // async-validator only reads `.message` when truthy: '' means no message shown
            // eslint-disable-next-line no-throw-literal
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
      throw new Error('Invalid query');
    }
  },
};
const JSON_VALIDATOR = {
  async validator(rule: ValidationRule, value: string) {
    try {
      BestEffortJsonParse(value);
    } catch {
      // async-validator only reads `.message` when truthy: '' means no message shown
      // eslint-disable-next-line no-throw-literal
      throw '';
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

  async function parseAndPrepareQuery(queryString: string, queryType: QueryType) {
    if (queryType === 'graphql') {
      return validateQuery(
        JSON.parse(await convertGraphQLToJsonQuery({ query: queryString, apiUrl, apiToken })) || {}
      );
    }

    return validateQuery(BestEffortJsonParse(queryString) || {});
  }

  const onJsonBlur = useCallback(async () => {
    const blurType = form.getFieldValue('type');

    await pause(100);

    // check if onblur was triggered by type switch, skip if so
    if (blurType !== 'json') {
      return;
    }

    const jsonQuery = form.getFieldValue('jsonQuery');
    let sanitizedQuery = {};
    try {
      sanitizedQuery = validateQuery(BestEffortJsonParse(jsonQuery));
    } catch {
      // do nothing
    }

    const sanitized = sanitizedQuery;

    form.setFieldValue('jsonQuery', JSON.stringify(sanitized, null, 2));
  }, [meta]);

  const onGraphqlBlur = useCallback(async () => {
    await pause(100);

    const graphqlQuery = form.getFieldValue('graphqlQuery');
    const blurType = form.getFieldValue('type');

    // check if onblur was triggered by type switch, skip if so
    if (blurType !== 'graphql') {
      return;
    }

    setIsBlocked(true);

    return convertGraphQLToJsonQuery({ query: graphqlQuery, apiUrl, apiToken })
      .then((jsonQuery) => {
        const jsonParsedQuery = validateQuery(JSON.parse(jsonQuery) || {});
        const convertedGraphqlQuery = convertJsonQueryToGraphQL({ meta, query: jsonParsedQuery });

        form.setFieldValue('graphqlQuery', convertedGraphqlQuery);
      })
      .finally(() => {
        setIsBlocked(false);
      });
  }, [meta]);

  const defaultQueryValue = useMemo(() => {
    if (type === 'json') {
      return JSON.stringify(query || {}, null, 2);
    }

    return meta && query ? convertJsonQueryToGraphQL({ meta, query }) : '';
  }, [type, meta, query]);

  const onTypeChange = useCallback((nextType) => {
    setType(nextType);
    const originalQuery = form.getFieldValue(nextType === 'json' ? 'graphqlQuery' : 'jsonQuery');
    setIsBlocked(true);

    parseAndPrepareQuery(originalQuery, nextType === 'json' ? 'graphql' : 'json')
      .then((parsedQuery) => {
        let value = '';

        if (nextType === 'json') {
          value = JSON.stringify(parsedQuery || {}, null, 2);
        } else if (parsedQuery) {
          value = convertJsonQueryToGraphQL({ meta, query: parsedQuery });
        }

        form.setFieldValue(nextType === 'json' ? 'jsonQuery' : 'graphqlQuery', value);
      })
      .catch((e) => {
        form.setFieldValue(
          nextType === 'json' ? 'jsonQuery' : 'graphqlQuery',
          nextType === 'json' ? '{}' : DEFAULT_GRAPHQL_QUERY
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

  const onSubmitLocal = useCallback(async ({ type: submittedType }) => {
    await (submittedType === 'json' ? onJsonBlur() : onGraphqlBlur());

    const rawQuery = submittedType === 'json' ? form.getFieldValue('jsonQuery') : form.getFieldValue('graphqlQuery');

    await parseAndPrepareQuery(rawQuery, submittedType).then((preparedQuery) => onSubmit(preparedQuery));
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
