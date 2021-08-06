import {
  Query,
  TimeDimensionBase,
  TransformedQuery,
} from '@cubejs-client/core';
import { AvailableMembers } from '@cubejs-client/react';
import {
  Alert,
  Button,
  Divider,
  Input,
  notification,
  Space,
  Tabs,
  Typography,
} from 'antd';
import { useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { CodeSnippet } from '../../atoms';
import { Box, Flex } from '../../grid';
import {
  useDeepEffect,
  useIsCloud,
  useIsMounted,
  useToggle,
  useToken,
} from '../../hooks';
import {
  getMembersByCube,
  getNameMemberPairs,
  request,
} from '../../shared/helpers';
import { Cubes } from './components/Cubes';
import { Members } from './components/Members';
import { TimeDimension } from './components/TimeDimension';
import {
  getPreAggregationDefinitionFromReferences,
  getPreAggregationReferences,
  PreAggregationReferences,
  updateQuery,
} from './utils';

const { Paragraph, Link } = Typography;
const { TabPane } = Tabs;

const MainWrapper = styled.div`
  flex-grow: 1;
  min-width: 0;
`;

const RightSidePanel = styled.div`
  max-width: 300px;
`;

type RollupDesignerProps = {
  apiUrl: string;
  transformedQuery: TransformedQuery;
  defaultQuery: Query;
  availableMembers: AvailableMembers;
};

export function RollupDesigner({
  apiUrl,
  defaultQuery,
  availableMembers,
  transformedQuery,
}: RollupDesignerProps) {
  const isMounted = useIsMounted();
  const isCloud = useIsCloud();
  const token = useToken();

  const canBeRolledUp =
    transformedQuery.leafMeasureAdditive &&
    !transformedQuery.hasMultipliedMeasures;

  const canUseMutex = useRef<number>(0);
  const [matching, setMatching] = useState<boolean>(false);
  const [saving, setSaving] = useState<boolean>(false);
  const [preAggName, setPreAggName] = useState<string>('main');
  const [isRollupCodeVisible, toggleRollupCode] = useToggle(!canBeRolledUp);

  const { order, limit, filters, ...matchedQuery } = defaultQuery;

  const [references, setReferences] = useState<PreAggregationReferences>(
    getPreAggregationReferences(transformedQuery)
  );

  const selectedKeys = useMemo(() => {
    const keys: string[] = [];

    ['measures', 'dimensions', 'timeDimensions'].map((memberKey) => {
      if (memberKey === 'timeDimensions') {
        const { dimension } = references[memberKey]?.[0] || {};

        if (dimension) {
          keys.push(dimension);
        }
      } else {
        references[memberKey]?.map((key) => keys.push(key));
      }
    });

    return keys;
  }, [references]);

  useDeepEffect(() => {
    let mutext = canUseMutex.current;
    const { measures, dimensions, timeDimensions } = references;

    async function load() {
      const { json } = await request(
        `${apiUrl}/pre-aggregations/can-use`,
        'POST',
        {
          token: token!,
          body: {
            transformedQuery,
            references: {
              measures,
              dimensions,
              timeDimensions,
            },
          },
        }
      );

      if (isMounted() && mutext === canUseMutex.current) {
        setMatching(json.canUsePreAggregationForTransformedQuery);
        canUseMutex.current++;
      }
    }

    if (token != null) {
      load();
    }
  }, [isMounted, references, token, canUseMutex]);

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

  async function handleAddToSchemaClick() {
    setSaving(true);

    const response = await request(
      '/playground/schema/pre-aggregation',
      'POST',
      {
        body: {
          preAggregationName: preAggName,
          cubeName,
          code: getPreAggregationDefinitionFromReferences(
            references,
            preAggName
          ).value,
        },
      }
    );

    setSaving(false);

    if (response.ok) {
      notification.success({
        message: `Pre-aggregation has been added to the ${cubeName} cube`,
      });
    } else {
      const { error } = response.json;
      notification.error({
        message: error,
      });
    }
  }

  function handleMemberToggle(memberType) {
    return (key) => {
      setReferences(updateQuery(references, memberType, key) as any);
    };
  }

  function rollupBody() {
    if (isRollupCodeVisible) {
      if (!canBeRolledUp) {
        return (
          <Paragraph>
            <Link
              href="https://cube.dev/docs/caching/pre-aggregations/getting-started#ensuring-pre-aggregations-are-targeted-by-queries"
              target="_blank"
            >
              Current query cannot be rolled up due to it is not additive
            </Link>
            . Please consider removing not additive measures like
            `countDistinct` or `avg`. You can also try to use{' '}
            <Link
              href="https://cube.dev/docs/pre-aggregations#parameters-type-originalsql"
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
            code={
              getPreAggregationDefinitionFromReferences(references, preAggName)
                .code
            }
          />

          <Flex justifyContent="flex-end" gap={2}>
            <Button onClick={toggleRollupCode}>Back to editing</Button>

            {!isCloud ? (
              <Button
                type="primary"
                loading={saving}
                onClick={handleAddToSchemaClick}
              >
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
    <Flex justifyContent="space-between" gap={2}>
      {!isRollupCodeVisible ? (
        <div>
          <Cubes
            selectedKeys={selectedKeys}
            membersByCube={getMembersByCube(availableMembers)}
            onSelect={(memberType, key) => {
              handleMemberToggle(memberType)(key);
            }}
          />
        </div>
      ) : null}

      <MainWrapper>
        <Space direction="vertical" style={{ width: '100%' }}>
          {!isRollupCodeVisible && (
            <Flex justifyContent="flex-end">
              <Button type="primary" onClick={toggleRollupCode}>
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
                    {references.measures?.length ? (
                      <>
                        <Members
                          title="Measures"
                          members={references.measures.map(
                            (name) => indexedMembers[name]
                          )}
                          onRemove={handleMemberToggle('measures')}
                        />

                        <Divider />
                      </>
                    ) : null}

                    {references.dimensions?.length ? (
                      <>
                        <Members
                          title="Dimensions"
                          members={references.dimensions.map(
                            (name) => indexedMembers[name]
                          )}
                          onRemove={handleMemberToggle('dimensions')}
                        />

                        <Divider />
                      </>
                    ) : null}

                    {references.timeDimensions.length ? (
                      <TimeDimension
                        member={
                          indexedMembers[references.timeDimensions[0].dimension]
                        }
                        granularity={references.timeDimensions[0].granularity}
                        onGranularityChange={(granularity) => {
                          setReferences({
                            ...references,
                            timeDimensions: [
                              {
                                ...(references.timeDimensions[0] || {}),
                                ...(granularity ? { granularity } : null),
                              } as TimeDimensionBase,
                            ],
                          });
                        }}
                        onRemove={handleMemberToggle('timeDimensions')}
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
        <Flex direction="column" gap={2}>
          {canBeRolledUp &&
            (matching ? (
              <Alert message="This pre-aggregation will match and accelerate this query:" />
            ) : (
              <Alert
                type="warning"
                message="This pre-aggregation will not match this query:"
              />
            ))}

          <CodeSnippet
            style={{ marginBottom: 16 }}
            code={JSON.stringify(matchedQuery, null, 2)}
          />
        </Flex>
      </RightSidePanel>
    </Flex>
  );
}
