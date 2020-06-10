import React, { Fragment, useState } from 'react';
import * as PropTypes from 'prop-types';
import { Row, Col, Divider, Card, Popover, Button } from 'antd';
import { SortAscendingOutlined } from '@ant-design/icons';
import { QueryBuilder } from '@cubejs-client/react';
import ChartRenderer from '../ChartRenderer';
import MemberGroup from './MemberGroup';
import FilterGroup from './FilterGroup';
import TimeGroup from './TimeGroup';
import SelectChartType from './SelectChartType';
import OrderGroup from './Order/OrderGroup';

export default function ExploreQueryBuilder({ vizState, cubejsApi, setVizState, chartExtra }) {
  const [isOrderPopoverVisible, toggleOrderPopover] = useState(false);

  return (
    <QueryBuilder
      vizState={vizState}
      setVizState={setVizState}
      cubejsApi={cubejsApi}
      wrapWithQueryRenderer={false}
      render={({
        validatedQuery,
        isQueryPresent,
        chartType,
        updateChartType,
        measures,
        availableMeasures,
        updateMeasures,
        dimensions,
        availableDimensions,
        updateDimensions,
        segments,
        availableSegments,
        updateSegments,
        filters,
        updateFilters,
        timeDimensions,
        availableTimeDimensions,
        updateTimeDimensions,
        orderMembers,
        updateOrder
      }) => {
        return (
          <Fragment>
            <Row type="flex" justify="space-around" align="top" gutter={24} style={{ marginBottom: 12 }}>
              <Col span={24}>
                <Card>
                  <Row type="flex" justify="space-around" align="top" gutter={24} style={{ marginBottom: 12 }}>
                    <Col span={24}>
                      <MemberGroup
                        members={measures}
                        availableMembers={availableMeasures}
                        addMemberName="Measure"
                        updateMethods={updateMeasures}
                      />
                      <Divider type="vertical" />
                      <MemberGroup
                        members={dimensions}
                        availableMembers={availableDimensions}
                        addMemberName="Dimension"
                        updateMethods={updateDimensions}
                      />
                      <Divider type="vertical" />
                      <MemberGroup
                        members={segments}
                        availableMembers={availableSegments}
                        addMemberName="Segment"
                        updateMethods={updateSegments}
                      />
                      <Divider type="vertical" />
                      <TimeGroup
                        members={timeDimensions}
                        availableMembers={availableTimeDimensions}
                        addMemberName="Time"
                        updateMethods={updateTimeDimensions}
                      />
                    </Col>
                  </Row>

                  <Row type="flex" justify="space-around" align="top" gutter={24} style={{ marginBottom: 12 }}>
                    <Col span={24}>
                      <FilterGroup
                        members={filters}
                        availableMembers={availableDimensions.concat(availableMeasures)}
                        addMemberName="Filter"
                        updateMethods={updateFilters}
                      />
                    </Col>
                  </Row>

                  <Row type="flex" justify="space-around" align="top" gutter={24}>
                    <Col span={24}>
                      <SelectChartType chartType={chartType} updateChartType={updateChartType} />

                      <Divider type="vertical" />

                      <Popover
                        content={
                          <OrderGroup
                            orderMembers={orderMembers}
                            onReorder={updateOrder.reorder}
                            onOrderChange={updateOrder.set}
                          />
                        }
                        visible={isOrderPopoverVisible}
                        placement="bottomLeft"
                        trigger="click"
                        onVisibleChange={(visible) => {
                          if (!visible) {
                            toggleOrderPopover(false);
                          } else {
                            if (orderMembers.length) {
                              toggleOrderPopover(!isOrderPopoverVisible);
                            }
                          }
                        }}
                      >
                        <Button disabled={!orderMembers.length} icon={<SortAscendingOutlined />}>
                          Order
                        </Button>
                      </Popover>
                    </Col>
                  </Row>
                </Card>
              </Col>
            </Row>

            <Row type="flex" justify="space-around" align="top" gutter={24}>
              <Col span={24}>
                {isQueryPresent ? (
                  <Card style={{ minHeight: 420 }} extra={chartExtra}>
                    <ChartRenderer
                      vizState={{
                        query: validatedQuery,
                        chartType,
                      }}
                      cubejsApi={cubejsApi}
                    />
                  </Card>
                ) : (
                  <h2 style={{ textAlign: 'center' }}>Choose a measure or dimension to get started</h2>
                )}
              </Col>
            </Row>
          </Fragment>
        );
      }}
    />
  );
}

ExploreQueryBuilder.propTypes = {
  vizState: PropTypes.object,
  setVizState: PropTypes.func,
  cubejsApi: PropTypes.object,
  chartExtra: PropTypes.array,
};

ExploreQueryBuilder.defaultProps = {
  vizState: {},
  setVizState: null,
  cubejsApi: null,
  chartExtra: null,
};
