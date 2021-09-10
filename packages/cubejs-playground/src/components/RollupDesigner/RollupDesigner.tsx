import { EditOutlined, WarningFilled } from '@ant-design/icons';
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
  Tabs,
  Typography,
  Space,
} from 'antd';
import { useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { CodeSnippet } from '../../atoms';
import { Box, Flex } from '../../grid';
import { useDeepEffect, useIsMounted, useToken } from '../../hooks';
import { useCloud } from '../../playground/cloud';
import { getNameMemberPairs, request } from '../../shared/helpers';
import { prettifyObject } from '../../utils';
import { Cubes } from './components/Cubes';
import { Members } from './components/Members';
import { RollupSettings, Settings } from './components/Settings';
import { TimeDimension } from './components/TimeDimension';
import {
  getRollupDefinitionFromReferences,
  getPreAggregationReferences,
  PreAggregationReferences,
  updateQuery,
} from './utils';

const { Paragraph, Link, Text } = Typography;
const { TabPane } = Tabs;

const MainBox = styled(Box)`
  & .ant-tabs-nav {
    padding-left: 24px;
    margin: 0;
  }
`;

const RollupQueryBox = styled.div`
  padding: 0 24px;
  background: var(--layout-body-background);
  width: 420px;
  min-width: 420px;

  & .ant-tabs-nav {
    margin-bottom: 24px;
  }

  & .ant-tabs-tab {
    div,
    .ant-typography {
      font-weight: 500;
    }
  }

  & .ant-typography > .anticon {
    padding-left: 12px;
  }
`;

type RollupDesignerProps = {
  apiUrl: string;
  transformedQuery: TransformedQuery;
  defaultQuery: Query;
  memberTypeCubeMap: AvailableMembers;
};

export function RollupDesigner({
  apiUrl,
  defaultQuery,
  memberTypeCubeMap,
  transformedQuery,
}: RollupDesignerProps) {
  const isMounted = useIsMounted();
  const token = useToken();
  const { isCloud, ...cloud } = useCloud();

  const [isCronValid, setCronValidity] = useState<boolean>(true);
  const [settings, setSettings] = useState<RollupSettings>({});

  const canBeRolledUp =
    transformedQuery.leafMeasureAdditive &&
    !transformedQuery.hasMultipliedMeasures;

  const canUseMutex = useRef<number>(0);
  const [matching, setMatching] = useState<boolean>(false);
  const [saving, setSaving] = useState<boolean>(false);
  const [preAggName, setPreAggName] = useState<string>('main');

  const { order, limit, filters, ...matchedQuery } = defaultQuery;

  const [timeDimension] = matchedQuery.timeDimensions || [];

  // There's nothing we can do to for a rollup to match such query
  const hideMatchRollupButton =
    timeDimension?.dimension &&
    !timeDimension?.dateRange &&
    !timeDimension?.granularity;

  const segments = new Set<string>();
  memberTypeCubeMap.segments.forEach(({ members }) => {
    members.forEach(({ name }) => segments.add(name));
  });

  const [references, setReferences] = useState<PreAggregationReferences>(
    getPreAggregationReferences(transformedQuery, segments)
  );

  const selectedKeys = useMemo(() => {
    const keys = new Set<string>();

    ['measures', 'dimensions', 'timeDimensions', 'segments'].map(
      (memberKey) => {
        if (memberKey === 'timeDimensions') {
          const { dimension } = references[memberKey]?.[0] || {};

          if (dimension) {
            keys.add(dimension);
          }
        } else {
          references[memberKey]?.map((key) => keys.add(key));
        }
      }
    );

    return Array.from(keys.values());
  }, [references]);

  useDeepEffect(() => {
    let mutex = canUseMutex.current;
    const { measures, segments, dimensions, timeDimensions } = references;

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
              dimensions: dimensions.concat(segments),
              timeDimensions,
            },
          },
        }
      );

      if (isMounted() && mutex === canUseMutex.current) {
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
      ...memberTypeCubeMap.measures,
      ...memberTypeCubeMap.dimensions,
      ...memberTypeCubeMap.timeDimensions,
      ...memberTypeCubeMap.segments,
    ])
  );

  async function handleAddToSchemaClick() {
    const definition = {
      preAggregationName: preAggName,
      cubeName,
      code: getRollupDefinitionFromReferences(references, preAggName, settings)
        .value,
    };

    function showSuccessMessage() {
      notification.success({
        message: `Rollup has been added to the ${cubeName} cube`,
      });
    }

    setSaving(true);

    if (!isCloud) {
      const response = await request(
        '/playground/schema/pre-aggregation',
        'POST',
        {
          body: definition,
        }
      );

      if (response.ok) {
        showSuccessMessage();
      } else {
        const { error } = response.json;
        notification.error({
          message: error,
        });
      }
    } else {
      if (cloud.addPreAggregationToSchema == null) {
        throw new Error('cloud.addPreAggregationToSchema is not defined');
      }

      const { error } = await cloud.addPreAggregationToSchema(definition);
      if (!error) {
        showSuccessMessage();
      } else {
        notification.error({
          message: error,
        });
      }
    }

    setSaving(false);
  }

  function handleSettingsChange(values) {
    const nextSettings: RollupSettings = {};

    if (values['refreshKey.checked.every']) {
      if (values['refreshKey.cron']) {
        nextSettings.refreshKey = {
          every: `\`${values['refreshKey.cron']}\``,
        };
      } else {
        nextSettings.refreshKey = {
          every: `\`${values['refreshKey.value']} ${values['refreshKey.granularity']}\``,
        };
      }
    }

    if (values['refreshKey.checked.sql'] && values['refreshKey.sql']) {
      nextSettings.refreshKey = {
        ...nextSettings.refreshKey,
        sql: `\`${values['refreshKey.sql']}\``,
      };
    }

    if (values.partitionGranularity) {
      nextSettings.partitionGranularity = `\`${values.partitionGranularity}\``;

      if (values['updateWindow.value']) {
        const value = [
          values['updateWindow.value'],
          values['updateWindow.granularity'],
        ].join(' ');

        nextSettings.refreshKey = {
          ...nextSettings.refreshKey,
          updateWindow: `\`${value}\``,
        };
      }

      nextSettings.refreshKey = {
        ...nextSettings.refreshKey,
        incremental: values['incrementalRefresh'],
      };
    }

    if (Array.isArray(values.indexes) && values.indexes.length > 0) {
      nextSettings.indexes = {
        indexName: {
          columns: values.indexes,
        },
      };
    }

    setSettings(nextSettings);
  }

  function handleMemberToggle(memberType) {
    return (key) => {
      setReferences(updateQuery(references, memberType, key) as any);
    };
  }

  function rollupBody() {
    if (!canBeRolledUp) {
      return (
        <Paragraph>
          <Link
            href="https://cube.dev/docs/caching/pre-aggregations/getting-started#ensuring-pre-aggregations-are-targeted-by-queries"
            target="_blank"
          >
            Current query cannot be rolled up due to it is not additive
          </Link>
          . Please consider removing not additive measures like `countDistinct`
          or `avg`. You can also try to use{' '}
          <Link
            href="https://cube.dev/docs/schema/reference/pre-aggregations#parameters-type-originalsql"
            target="_blank"
          >
            originalSql
          </Link>{' '}
          rollup instead.
        </Paragraph>
      );
    }

    return (
      <>
        <CodeSnippet
          style={{ marginBottom: 16 }}
          code={
            getRollupDefinitionFromReferences(references, preAggName, settings)
              .code
          }
          copyMessage="Rollup definition is copied"
          theme="light"
        />

        <Button
          type="primary"
          loading={saving}
          disabled={!isCronValid}
          style={{ width: '100%' }}
          onClick={handleAddToSchemaClick}
        >
          Add to the Data Schema
        </Button>
      </>
    );
  }

  return (
    <Flex justifyContent="space-between" margin={[0, 0, 2, 0]}>
      <MainBox grow={1}>
        <Tabs style={{ minHeight: '100%' }}>
          <TabPane tab="Members" key="members">
            <Flex gap={2}>
              <Box style={{ minWidth: 256 }}>
                <Cubes
                  selectedKeys={selectedKeys}
                  memberTypeCubeMap={memberTypeCubeMap}
                  onSelect={(memberType, key) => {
                    handleMemberToggle(memberType)(key);
                  }}
                />
              </Box>

              <Box
                grow={1}
                style={{
                  marginTop: 24,
                }}
              >
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

                {references.segments.length ? (
                  <>
                    <Members
                      title="Segments"
                      members={references.segments.map(
                        (name) => indexedMembers[name]
                      )}
                      onRemove={handleMemberToggle('segments')}
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
              </Box>
            </Flex>
          </TabPane>

          <TabPane tab="Settings" key="settings">
            <Settings
              hasTimeDimension={references.timeDimensions.length > 0}
              members={references.measures
                .concat(references.dimensions)
                .concat(references.timeDimensions.map((td) => td.dimension))}
              onCronExpressionValidityChange={setCronValidity}
              onChange={handleSettingsChange}
            />
          </TabPane>
        </Tabs>
      </MainBox>

      <RollupQueryBox>
        <Tabs>
          <TabPane tab="Rollup Definition" key="rollup">
            <Flex
              direction="column"
              justifyContent="flex-start"
              style={{ marginBottom: 64 }}
            >
              {!references.timeDimensions.length && (
                <Box style={{ marginBottom: 24 }}>
                  <Alert
                    type="warning"
                    message="This rollup has no time dimension so it cannot be partitioned"
                  />
                </Box>
              )}

              {canBeRolledUp ? (
                <Box style={{ marginBottom: 16 }}>
                  <Paragraph>
                    Add the following rollup pre-aggregation
                    <br /> to the <b>{cubeName}</b> cube:
                  </Paragraph>

                  <Paragraph style={{ margin: '24px 0 4px' }}>
                    Rollup Name
                  </Paragraph>

                  <Input
                    value={preAggName}
                    suffix={<EditOutlined />}
                    onChange={(event) => setPreAggName(event.target.value)}
                  />
                </Box>
              ) : null}

              <Box>{rollupBody()}</Box>
            </Flex>
          </TabPane>

          <TabPane
            tab={
              canBeRolledUp && matching ? (
                'Query Compatibility'
              ) : (
                <Typography.Text>
                  Query Compatibility
                  <WarningFilled style={{ color: '#FBBC05' }} />
                </Typography.Text>
              )
            }
            key="query"
          >
            <Flex direction="column" justifyContent="flex-start">
              <Box style={{ marginBottom: 32 }}>
                {canBeRolledUp && matching ? (
                  <Text>This rollup will match the following query:</Text>
                ) : (
                  <Space direction="vertical">
                    <Alert
                      type="warning"
                      message={
                        <Text>
                          This rollup does <b>NOT</b> match the following query:
                        </Text>
                      }
                    />

                    {!hideMatchRollupButton && (
                      <Button
                        type="primary"
                        ghost
                        onClick={() => {
                          setReferences(
                            getPreAggregationReferences(
                              transformedQuery,
                              segments
                            )
                          );
                          setMatching(true);
                        }}
                      >
                        Match Rollup
                      </Button>
                    )}
                  </Space>
                )}
              </Box>

              <CodeSnippet
                style={{ minWidth: 200 }}
                code={prettifyObject(matchedQuery)}
                copyMessage="Query is copied"
                theme="light"
              />
            </Flex>
          </TabPane>
        </Tabs>
      </RollupQueryBox>
    </Flex>
  );
}
