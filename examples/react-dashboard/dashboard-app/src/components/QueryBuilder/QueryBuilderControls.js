import React from "react";
import { Row, Col, Divider } from "antd";
import MemberGroup from "./MemberGroup";
import FilterGroup from "./FilterGroup";
import TimeGroup from "./TimeGroup";

import styled from 'styled-components';

const ControlsWrap = styled(Row)`
  background: #ffffff;
  margin-bottom: 12px;
  padding: 18px 28px 10px 28px;
`

const StyledDivider = styled(Divider)`
  margin: 0 12px;
  height: 4.5em;
  top: 0.5em;
  background: #F4F5F6;
`

const HorizontalDivider = styled(Divider)`
  padding: 0;
  margin: 0;
  background: #F4F5F6;
`

const QueryBuilderControls = ({
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
  isQueryPresent
}) => (
  <ControlsRow type="flex" justify="space-around" align="top" key="1">
    <Col span={24}>
      <Row type="flex" align="top" style={{ paddingBottom: 23}}>
        <MemberGroup
          title="Measures"
          members={measures}
          availableMembers={availableMeasures}
          addMemberName="Measure"
          updateMethods={updateMeasures}
        />
        <StyledDivider type="vertical" />
        <MemberGroup
          title="Dimensions"
          members={dimensions}
          availableMembers={availableDimensions}
          addMemberName="Dimension"
          updateMethods={updateDimensions}
        />
        <StyledDivider type="vertical"/>
        <MemberGroup
          title="Segments"
          members={segments}
          availableMembers={availableSegments}
          addMemberName="Segment"
          updateMethods={updateSegments}
        />
        <StyledDivider type="vertical"/>
        <TimeGroup
          title="Time"
          members={timeDimensions}
          availableMembers={availableTimeDimensions}
          addMemberName="Time"
          updateMethods={updateTimeDimensions}
        />
      </Row>
      {!!isQueryPresent && ([
        <HorizontalDivider />,
        <Row type="flex" justify="space-around" align="top" gutter={24} style={{ marginTop: 10 }}>
          <Col span={24}>
            <FilterGroup
              members={filters}
              availableMembers={availableDimensions.concat(availableMeasures)}
              addMemberName="Filter"
              updateMethods={updateFilters}
            />
          </Col>
        </Row>
      ])}
    </Col>
  </ControlsRow>
);

export default QueryBuilderControls;
