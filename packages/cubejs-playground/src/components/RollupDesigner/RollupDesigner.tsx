import { Alert, Button, Input, Space, Tabs, Typography, Divider } from 'antd';
import { Query, TransformedQuery } from '@cubejs-client/core';
import styled from 'styled-components';
import { useState } from 'react';
import { camelCase } from 'camel-case';

import { CodeSnippet } from '../../atoms';
import { getPreAggregationDefinition, PreAggregationDefinition } from './utils';
import { useToggle } from '../../hooks';
import { AvailableMembers } from '@cubejs-client/react';
import { getMembersByCube, getNameMemberPairs } from '../../shared/helpers';
import { Cubes } from './components/Cubes';
import { Members } from './components/Members';
import { TimeDimension } from './components/TimeDimension';

const { Paragraph } = Typography;
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

const Flex = styled.div`
  display: flex;
  justify-content: space-between;
  gap: 20px;
`;

type RollupDesignerProps = {
  query: Query;
  transformedQuery: TransformedQuery;
  availableMembers: AvailableMembers;
};

export function RollupDesigner({
  query,
  availableMembers,
  transformedQuery,
}: RollupDesignerProps) {
  const [preAggName, setPreAggName] = useState<string>('main');
  const [isRollupCodeVisible, toggleRollupCode] = useToggle();

  let preAggregation: null | PreAggregationDefinition = null;

  if (
    transformedQuery.leafMeasureAdditive &&
    !transformedQuery.hasMultipliedMeasures
  ) {
    preAggregation = getPreAggregationDefinition(
      transformedQuery,
      camelCase(preAggName)
    );
  }

  const indexedMembers = Object.fromEntries(
    getNameMemberPairs([
      ...availableMembers.measures,
      ...availableMembers.dimensions,
      ...availableMembers.timeDimensions,
    ])
  );

  const cubeName =
    transformedQuery &&
    (
      transformedQuery.leafMeasures[0] ||
      transformedQuery.sortedDimensions[0] ||
      'your'
    ).split('.')[0];

  const { order, limit, filters, ...matchedQuery } = query;

  return (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Wrapper>
        <div>
          <Cubes membersByCube={getMembersByCube(availableMembers)} />
        </div>

        <MainWrapper>
          <Space direction="vertical" style={{ width: '100%' }}>
            <Flex>
              <Button type="primary" onClick={toggleRollupCode}>
                {isRollupCodeVisible
                  ? 'Back to editing'
                  : 'Preview rollup definition'}
              </Button>

              {isRollupCodeVisible ? (
                <Input
                  value={preAggName}
                  onChange={(event) => setPreAggName(event.target.value)}
                />
              ) : null}
            </Flex>

            <Tabs>
              <TabPane tab="Members" key="members">
                {preAggregation ? (
                  isRollupCodeVisible ? (
                    <>
                      <Paragraph>
                        Add the following pre-aggregation to the{' '}
                        <b>{cubeName}</b> cube.
                      </Paragraph>

                      <CodeSnippet
                        style={{ marginBottom: 16 }}
                        code={preAggregation.code}
                      />
                    </>
                  ) : (
                    <div>
                      <Members
                        title="Measures"
                        members={preAggregation.measures.map((name) => {
                          return indexedMembers[name];
                        })}
                      />

                      <Divider />

                      <Members
                        title="Dimensions"
                        members={preAggregation.dimensions.map((name) => {
                          return indexedMembers[name];
                        })}
                      />

                      <Divider />

                      {preAggregation.timeDimension ? (
                        <TimeDimension
                          member={indexedMembers[preAggregation.timeDimension]}
                          granularity={preAggregation.granularity}
                        />
                      ) : null}
                    </div>
                  )
                ) : null}
              </TabPane>

              <TabPane tab="Settings" key="settings">
                settings
              </TabPane>

              <TabPane tab="Queries" key="queries">
                queries
              </TabPane>
            </Tabs>
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

  // return (
  //   <>
  //     {preAggregation?.code ? (
  //       <>
  //         <Paragraph>
  //           Add the following pre-aggregation to the <b>{cubeName}</b> cube.
  //         </Paragraph>
  //
  //         <CodeSnippet style={{ marginBottom: 16 }} code={preAggregation.code} />
  //       </>
  //     ) : (
  //       <Paragraph>
  //         <Link
  //           href="!https://cube.dev/docs/pre-aggregations#rollup-rollup-selection-rules"
  //           target="_blank"
  //         >
  //           Current query cannot be rolled up due to it is not additive
  //         </Link>
  //         . Please consider removing not additive measures like `countDistinct`
  //         or `avg`. You can also try to use{' '}
  //         <Link
  //           href="!https://cube.dev/docs/pre-aggregations#original-sql"
  //           target="_blank"
  //         >
  //           originalSql
  //         </Link>{' '}
  //         pre-aggregation instead.
  //       </Paragraph>
  //     )}
  //
  //     <Link
  //       style={{ paddingTop: 16 }}
  //       href="https://cube.dev/docs/caching/pre-aggregations/getting-started"
  //       target="_blank"
  //     >
  //       Further reading about pre-aggregations for reference.
  //     </Link>
  //   </>
  // );
}
