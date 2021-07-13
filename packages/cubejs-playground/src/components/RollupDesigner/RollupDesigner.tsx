import { Alert, Button, Input, Space, Tabs, Typography, Divider, notification } from 'antd';
import {
  Query,
  TimeDimensionBase,
  TransformedQuery,
} from '@cubejs-client/core';
import styled from 'styled-components';
import { useEffect, useMemo, useState } from 'react';
import { camelCase } from 'camel-case';
import { AvailableMembers, useLazyDryRun } from '@cubejs-client/react';

import { CodeSnippet } from '../../atoms';
import { Flex, Box } from '../../grid';
import { useIsCloud } from '../AppContext';
import {
  getPreAggregationDefinition,
  PreAggregationDefinition,
  updateQuery,
} from './utils';
import { useToggle } from '../../hooks';
import { getMembersByCube, getNameMemberPairs } from '../../shared/helpers';
import { Cubes } from './components/Cubes';
import { Members } from './components/Members';
import { TimeDimension } from './components/TimeDimension';

const { Paragraph, Link } = Typography;
const { TabPane } = Tabs;

const Wrapper = styled.div`
  display: flex;
  justify-content: space-between;
  gap: 20px;
`;

const MainWrapper = styled.div`
  flex-grow: 1;
`;

const RightSidePanel = styled.div`
  max-width: 300px;
`;

// const Flex = styled.div`
//   display: flex;
//   justify-content: space-between;
//   gap: 20px;
//   margin-bottom: 16px;
// `;

type RollupDesignerProps = {
  defaultQuery: Query;
  defaultTransformedQuery: TransformedQuery;
  availableMembers: AvailableMembers;
};

export function RollupDesigner({
  defaultQuery,
  availableMembers,
  defaultTransformedQuery,
}: RollupDesignerProps) {
  const [load, { isLoading, response, error }] = useLazyDryRun();
  const isCloud = useIsCloud();

  const [query, setQuery] = useState<Query>(defaultQuery);
  const [transformedQuery, setTransformedQuery] = useState<TransformedQuery>(
    defaultTransformedQuery
  );
  const [saving, setSaving] = useState<boolean>(false);
  const [preAggName, setPreAggName] = useState<string>('main');
  const [isRollupCodeVisible, toggleRollupCode] = useToggle();

  let preAggregation: null | PreAggregationDefinition = null;

  const { order, limit, filters, ...matchedQuery } = query;

  const selectedKeys = useMemo(() => {
    const keys: string[] = [];

    ['measures', 'dimensions', 'timeDimensions'].map((memberKey) => {
      if (memberKey === 'timeDimensions') {
        const { dimension } = query[memberKey]?.[0] || {};

        if (dimension) {
          keys.push(dimension);
        }
      } else {
        query[memberKey]?.map((key) => keys.push(key));
      }
    });

    return keys;
  }, [query]);

  useEffect(() => {
    if (!isLoading && response) {
      setTransformedQuery(response.transformedQueries[0]);
    }
  }, [isLoading, response]);

  if (
    transformedQuery.leafMeasureAdditive &&
    !transformedQuery.hasMultipliedMeasures
  ) {
    preAggregation = getPreAggregationDefinition(
      transformedQuery,
      camelCase(preAggName)
    );
  }

  const cubeName =
    transformedQuery &&
    (
      transformedQuery.leafMeasures[0] ||
      transformedQuery.sortedDimensions[0] ||
      'your'
    ).split('.')[0];

  const indexedMembers = Object.fromEntries(
    getNameMemberPairs([
      ...availableMembers.measures,
      ...availableMembers.dimensions,
      ...availableMembers.timeDimensions,
    ])
  );

  async function handleRollupButtonClick() {
    await load({ query });
    toggleRollupCode();
  }


  async function handleAddToSchemaClick() {
    setSaving(true);

    const response = await fetch('/playground/schema/pre-aggregation', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        preAggregationName: preAggName,
        cubeName,
        code: preAggregation?.value || ''
      }),
    });

    setSaving(false);

    if (response.ok) {
      notification.success({
        message: `Pre-aggregation has been added to the ${cubeName} cube`,
      });
    } else {
      const { error } = await response.json();
      notification.error({
        message: error
      });
    }
  }

  function handleMemberRemove(memberType) {
    return (key) => setQuery(updateQuery(query, memberType, key));
  }

  function rollupBody() {
    if (isRollupCodeVisible) {
      if (error) {
        return <Alert type="error" message={error.toString()} />;
      }

      if (!preAggregation) {
        return (
          <Paragraph>
            <Link
              href="https://cube.dev/docs/pre-aggregations#rollup-rollup-selection-rules"
              target="_blank"
            >
              Current query cannot be rolled up due to it is not additive
            </Link>
            . Please consider removing not additive measures like
            `countDistinct` or `avg`. You can also try to use{' '}
            <Link
              href="https://cube.dev/docs/pre-aggregations#original-sql"
              target="_blank"
            >
              originalSql
            </Link>{' '}
            pre-aggregation instead.
          </Paragraph>
        );
      }

      return (
        <div>
          <Paragraph>
            Add the following pre-aggregation to the <b>{cubeName}</b> cube.
          </Paragraph>

          <CodeSnippet
            style={{ marginBottom: 16 }}
            code={preAggregation.code}
          />

          <Flex justifyContent="flex-end" gap={2}>
            <Button onClick={toggleRollupCode}>Back to editing</Button>

            {!isCloud ? (
              <Button type="primary" loading={saving} onClick={handleAddToSchemaClick}>
                Add to the Data Schema
              </Button>
            ) : null}
          </Flex>
        </div>
      );
    }

    return null;
  }

  return (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Wrapper>
        {!isRollupCodeVisible ? (
          <div>
            <Cubes
              selectedKeys={selectedKeys}
              membersByCube={getMembersByCube(availableMembers)}
              onSelect={(memberType, key) => {
                setQuery(updateQuery(query, memberType, key));
              }}
            />
          </div>
        ) : null}

        <MainWrapper>
          <Space direction="vertical" style={{ width: '100%' }}>
            {!isRollupCodeVisible && (
              <Flex justifyContent="flex-end">
                <Button type="primary" onClick={handleRollupButtonClick}>
                  Preview rollup definition
                </Button>
              </Flex>
            )}

            <Flex direction="column" gap={2}>
              {isRollupCodeVisible ? (
                <Input
                  value={preAggName}
                  onChange={(event) => setPreAggName(event.target.value)}
                />
              ) : null}

              <Box>{rollupBody()}</Box>
            </Flex>

            {!isRollupCodeVisible ? (
              <Tabs>
                <TabPane tab="Members" key="members">
                  {!isRollupCodeVisible ? (
                    <div>
                      {query.measures?.length ? (
                        <>
                          <Members
                            title="Measures"
                            members={query.measures.map(
                              (name) => indexedMembers[name]
                            )}
                            onRemove={handleMemberRemove('measures')}
                          />

                          <Divider />
                        </>
                      ) : null}

                      {query.dimensions?.length ? (
                        <>
                          <Members
                            title="Dimensions"
                            members={query.dimensions.map(
                              (name) => indexedMembers[name]
                            )}
                            onRemove={handleMemberRemove('dimensions')}
                          />

                          <Divider />
                        </>
                      ) : null}

                      {query.timeDimensions?.length ? (
                        <TimeDimension
                          member={
                            indexedMembers[query.timeDimensions[0]?.dimension]
                          }
                          granularity={query.timeDimensions[0]?.granularity}
                          onGranularityChange={(granularity) => {
                            setQuery({
                              ...query,
                              timeDimensions: [
                                {
                                  ...(query.timeDimensions?.[0] || {}),
                                  ...(granularity ? { granularity } : null),
                                } as TimeDimensionBase,
                              ],
                            });
                          }}
                          onRemove={handleMemberRemove('timeDimensions')}
                        />
                      ) : null}
                    </div>
                  ) : null}
                </TabPane>

                {/*<TabPane tab="Settings" key="settings">*/}
                {/*  settings*/}
                {/*</TabPane>*/}

                {/*<TabPane tab="Queries" key="queries">*/}
                {/*  queries*/}
                {/*</TabPane>*/}
              </Tabs>
            ) : null}
          </Space>
        </MainWrapper>

        <RightSidePanel>
          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Alert message="This pre-aggregation will match and accelerate this query:" />

            <CodeSnippet
              style={{ marginBottom: 16 }}
              code={JSON.stringify(matchedQuery, null, 2)}
            />
          </Space>
        </RightSidePanel>
      </Wrapper>
    </Space>
  );
}
