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
} from 'antd';
import { useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { CodeSnippet } from '../../atoms';
import { Box, Flex } from '../../grid';
import { useDeepEffect, useIsMounted, useToken } from '../../hooks';
import { useCloud } from '../../playground/cloud';
import {
  getMembersByCube,
  getNameMemberPairs,
  request,
} from '../../shared/helpers';
import { prettifyObject } from '../../utils';
import { Cubes } from './components/Cubes';
import { Members } from './components/Members';
import { RollupSettings, Settings } from './components/Settings';
import { TimeDimension } from './components/TimeDimension';
import {
  getPreAggregationDefinitionFromReferences,
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

  & div.ant-typography {
    color: #14144680;
  }

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
  availableMembers: AvailableMembers;
};

export function RollupDesigner({
  apiUrl,
  defaultQuery,
  availableMembers,
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

  const segments = new Set<string>();
  availableMembers.segments.forEach(({ members }) => {
    members.forEach(({ name }) => segments.add(name));
  });

  const [references, setReferences] = useState<PreAggregationReferences>(
    getPreAggregationReferences(transformedQuery, segments)
  );

  const selectedKeys = useMemo(() => {
    const keys: string[] = [];

    ['measures', 'dimensions', 'timeDimensions', 'segments'].map(
      (memberKey) => {
        if (memberKey === 'timeDimensions') {
          const { dimension } = references[memberKey]?.[0] || {};

          if (dimension) {
            keys.push(dimension);
          }
        } else {
          references[memberKey]?.map((key) => keys.push(key));
        }
      }
    );

    return keys;
  }, [references]);

  useDeepEffect(() => {
    let mutext = canUseMutex.current;
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
      ...availableMembers.segments,
    ])
  );

  async function handleAddToSchemaClick() {
    const definition = {
      preAggregationName: preAggName,
      cubeName,
      code: getPreAggregationDefinitionFromReferences(
        references,
        preAggName,
        settings
      ).value,
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
            href="https://cube.dev/docs/caching/rollups/getting-started#ensuring-rollups-are-targeted-by-queries"
            target="_blank"
          >
            Current query cannot be rolled up due to it is not additive
          </Link>
          . Please consider removing not additive measures like `countDistinct`
          or `avg`. You can also try to use{' '}
          <Link
            href="https://cube.dev/docs/rollups#parameters-type-originalsql"
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
            getPreAggregationDefinitionFromReferences(
              references,
              preAggName,
              settings
            ).code
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
        <Tabs>
          <TabPane tab="Members" key="members">
            <Flex gap={2}>
              <Box style={{ minWidth: 256 }}>
                <Cubes
                  selectedKeys={selectedKeys}
                  membersByCube={getMembersByCube(availableMembers)}
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
              <Box style={{ marginBottom: 16 }}>
                {canBeRolledUp && matching ? (
                  <Typography.Paragraph>
                    This rollup will match the following query:
                  </Typography.Paragraph>
                ) : (
                  <Alert
                    type="warning"
                    message={
                      <Text>
                        This rollup does <b>NOT</b> match the following query:
                      </Text>
                    }
                  />
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
