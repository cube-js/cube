---
title: "Query Builder"
order: 5
---

In this part, we're going to make a lot of changes to style our query builder
component. Feel free to skip this part if you don't need to style it. Below, you
can see the final design of the query builder, which we'll have by the end of this
part.

`video: /videos/5-video.mp4`

The query builder component in the template, `<ExploreQueryBuilder />`, is built based on the
`<QueryBuilder />` component from the `@cubejs-client/react` package. The `<QueryBuilder />` abstracts state management and API calls to Cube.js Backend. It uses render prop and doesn’t render anything itself, but calls the render function instead. This way it gives maximum flexibility to building a custom-tailored UI with a minimal API.

In our dashboard template, we have a lot of small components that render
various query builder controls, such as measures/dimensions selects, filters,
chart types select, etc. We'll go over each of them to apply new styles.

`<ExploreQueryBuilder />` is a parent component, which first renders
all the controls we need to build our query: measures, dimensions,
segments, time, filters, and chart type selector controls. Then it renders the chart itself.
It also provides a basic layout of all the controls.

Let's start with customizing this component first and then we'll update all the
smaller components one by one. Replace the content of `src/components/QueryBuilder/ExploreQueryBuilder.js`
with the following.

```jsx
import React from "react";
import { Row, Col, Card, Divider } from "antd";
import styled from 'styled-components';
import { QueryBuilder } from "@cubejs-client/react";
import MemberGroup from "./MemberGroup";
import FilterGroup from "./FilterGroup";
import TimeGroup from "./TimeGroup";
import ChartRenderer from "../ChartRenderer";
import SelectChartType from './SelectChartType';

const ControlsRow = styled(Row)`
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

const ChartCard = styled(Card)`
  border-radius: 4px;
  border: none;
`

const ChartRow = styled(Row)`
  padding-left: 28px;
  padding-right: 28px;
`

const ExploreQueryBuilder = ({
  vizState,
  cubejsApi,
  setVizState,
  chartExtra
}) => (
  <QueryBuilder
    vizState={vizState}
    setVizState={setVizState}
    cubejsApi={cubejsApi}
    wrapWithQueryRenderer={false}
    render={({
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
      isQueryPresent,
      chartType,
      updateChartType,
      validatedQuery,
      cubejsApi
    }) => [
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
      </ControlsRow>,
      <ChartRow type="flex" justify="space-around" align="top" gutter={24} key="2">
        <Col span={24}>
          {isQueryPresent ? ([
            <Row style={{ marginTop: 15, marginBottom: 25 }}>
              <SelectChartType
                chartType={chartType}
                updateChartType={updateChartType}
              />
            </Row>,
            <ChartCard style={{ minHeight: 420 }}>
              <ChartRenderer
                vizState={{ query: validatedQuery, chartType }}
                cubejsApi={cubejsApi}
              />
            </ChartCard>
          ]) : (
            <h2
              style={{
                textAlign: "center"
              }}
            >
              Choose a measure or dimension to get started
            </h2>
          )}
        </Col>
      </ChartRow>
    ]}
  />
);
export default ExploreQueryBuilder;
```

There is a lot of code, but it's all about styling and rendering our layout with components' controls. Here we can see the following components, which render controls: `<MemberGroup />`, `<TimeGroup />`,  `<FilterGroup />`, and `<SelectChartType />`. There is also a `<ChartRenderer />`, which we will update in the next part.

Now, let's customize each of the components we render here. The first one is a
`<MemberGroup />`. It is used to render measures, dimensions, and segments.

Replace the content of the `src/components/QueryBuilder/MemberGroup.js` file
with the following.

```jsx
import React from 'react';
import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import MemberGroupTitle from './MemberGroupTitle';
import PlusIcon from './PlusIcon';

const MemberGroup = ({
  members, availableMembers, addMemberName, updateMethods, title
}) => (
  <div>
    <MemberGroupTitle title={title} />
    {members.map(m => (
      <RemoveButtonGroup key={m.index || m.name} onRemoveClick={() => updateMethods.remove(m)}>
        <MemberDropdown type="selected" availableMembers={availableMembers} onClick={updateWith => updateMethods.update(m, updateWith)}>
          {m.title}
        </MemberDropdown>
      </RemoveButtonGroup>
    ))}
    <MemberDropdown
      type={members.length > 0 ? "icon" : "new"}
      onClick={m => updateMethods.add(m)} availableMembers={availableMembers}
    >
      {addMemberName}
      <PlusIcon />
    </MemberDropdown>
  </div>
);

export default MemberGroup;
```

Here we can see that `<MemberGroup />` internally uses 4 main components to
render the control: `<MemberDropdown />`, `<RemoveButtonGroup />`,
`<MemberGroupTitle />`, and `<PlusIcon />`. Let's go over each of them.

We already have a `<MemberDropdown />` component in place. If you inspect it, you
will see that it uses `<ButtonDropdown />` internally to render the button for
the control. We are not going to customize `<MemberDropdown />`, but will do
customization on the button instead.

Replace the content of `src/components/QueryBuilder/ButtonDropdown.js` with the
following.


```jsx
import React from 'react';
import { Button, Dropdown } from 'antd';

import PlusIcon from './PlusIcon';

import styled from 'styled-components';

const StyledButton = styled(Button)`
  font-size: 14px;
  height: 48px;
  line-height: 3.5;
  box-shadow: 0px 2px 12px rgba(67, 67, 107, 0.1);
  border: none;
  color: #43436B;
  //animation-duration: 0s;


  &:hover + a {
    display: block;
  }

  &:hover, &.ant-dropdown-open, &:focus {
    color: #43436B;
  }

  &:after {
    animation: 0s;
  }

  & > i {
    position: relative;
    top: 3px;
  }
`

const SelectedFilterButton = styled(StyledButton)`
  && {
    height: 40px;
    line-height: 40px;
    box-shadow: none;
    border: 1px solid #ECECF0;
    border-radius: 4px;
  }
`

const NewButton = styled(StyledButton)`
  color: #7471f2;
  border: 1px solid rgba(122, 119, 255, 0.2);
  box-shadow: none;
  font-weight: bold;

  &:hover, &.ant-dropdown-open, &:focus {
    color: #6D5AE5;
    border-color: rgba(122, 119, 255, 0.2);
  }
`

const TimeGroupButton = styled(NewButton)`
  border: none;
  padding: 0;
`

const PlusIconButton = styled.span`
  margin-left: 12px;
  top: 5px;
  position: relative;
`

const ButtonDropdown = ({ overlay, type, ...buttonProps }) => {
  let component;
  if (type === 'icon') {
    component = <PlusIconButton><PlusIcon /></PlusIconButton>;
  } else if (type === 'selected') {
    component = <StyledButton {...buttonProps} />;
  } else if (type === 'time-group') {
    component = <TimeGroupButton {...buttonProps} />;
  } else if (type === 'selected-filter') {
    component = <SelectedFilterButton {...buttonProps} />;
  } else {
    component =  <NewButton {...buttonProps} />;
  }

  return (
    <Dropdown overlay={overlay} placement="bottomLeft" trigger={['click']}>
      { component }
    </Dropdown>
 )
}

export default ButtonDropdown;
```

There are a lot of changes, but as you can see, mostly around the styles of the
button. Depending on different states of the control, such as whether we already
have selected a member or not, we are changing the style of the button.

Now, let's go back to our list of components to style; the next one is `<RemoveButtonGroup />`. It is quite a simple component that renders a button to remove selected measures or dimensions. As mentioned at the beginning of this part, the `<QueryBuilder />` component from the `@cubejs-client/react` package takes care of all the logic and state, and we just need to render controls to perform actions.

Replace the content of `src/components/QueryBuilder/RemoveButtonGroup.js`
with the following.

```jsx
import React from 'react';
import { Button } from 'antd';
import removeButtonSvg from './remove-button.svg';

import styled from 'styled-components';

const StyledButton = styled.a`
  height: 16px;
  width: 16px;
  background-image: url(${removeButtonSvg});
  display: block;
  position: absolute;
  right: -5px;
  top: -5px;
  z-index: 9;
  display: none;

  &:hover {
    background-position: 16px 0;
    display: block;
  }
`

const RemoveButtonGroup = ({ onRemoveClick, children, display, ...props }) => (
  <Button.Group style={{ marginRight: 8 }} {...props}>
    {children}
    <StyledButton onClick={onRemoveClick} />
  </Button.Group>
);

export default RemoveButtonGroup;
```

Here we use an SVG image for our button. If you follow our design, you can download
it with the following command.

```bash
$ cd dashboard-app/src/components/QueryBuilder && curl http://cube.dev/downloads/remove-button.svg > remove-button.svg
```

The next component, `<MemberGroupTitle />`, doesn't add any functionality to our
query builder, but just acts as a label for our controls.

Let's create `src/components/QueryBuilder/MemberGroupTitle.js` with the
following content.


```jsx
import React from 'react';

import styled from 'styled-components';

const LabelStyled = styled.div`
  margin-bottom: 12px;
  color: #A1A1B5;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  font-size: 11px;
  font-weight: bold;
`
const MemberGroupTitle = ({ title }) => (
  <LabelStyled>{title}</LabelStyled>
);

export default MemberGroupTitle;
```

The last component from `<MemberGroup />` is `<PlusIcon />`. It just renders the
plus icon, which is used in all our controls.

Create `src/components/QueryBuilder/PlusIcon.js` with the
following content.

```jsx
import React from 'react';
import { Icon } from 'antd';
import { ReactComponent as PlusIconSvg } from './plus-icon.svg';

import styled from 'styled-components';

const PlusIconStyled = styled(Icon)`
  display: inline-block;
  background: #6F76D9;
  border-radius: 50%;
  width: 20px;
  height: 20px;
  position: relative;
  cursor: pointer;
  pointer-events: all !important;

  &::after {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: rgba(122,119,255,0.1);
    border-radius: 50%;
    transition: transform 0.15s cubic-bezier(0.0, 0.0, 0.2, 1);
    z-index: 1;
  }

  &:hover::after {
    transform: scale(1.4);
  }

  & svg {
    width: 20px;
    height: 20px;
    z-index: 2;
  }
`

const PlusIcon = () => (
   <PlusIconStyled component={PlusIconSvg} />
);

export default PlusIcon;
```

Same as for the remove button icon, you can download an SVG of the plus icon with
the following command.

```bash
$ cd dashboard-app/src/components/QueryBuilder && curl http://cube.dev/downloads/plus-icon.svg > plus-icon.svg
```

Now, we have all the components for the `<MemberGroup />` and we are almost done
with the controls styling. Next, let's update the `<TimeGroup />`, `<FilterGroup
/>`, and `<SelectChartType />` components.

Replace the contents of `src/components/QueryBuilder/TimeGroup.js` with the
following.

```jsx
import React from 'react';
import {
  Menu
} from 'antd';
import ButtonDropdown from './ButtonDropdown';
import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import MemberGroupTitle from './MemberGroupTitle';
import PlusIcon from './PlusIcon';
import styled from 'styled-components';

const DateRanges = [
  { title: 'All time', value: undefined },
  { value: 'Today' },
  { value: 'Yesterday' },
  { value: 'This week' },
  { value: 'This month' },
  { value: 'This quarter' },
  { value: 'This year' },
  { value: 'Last 7 days' },
  { value: 'Last 30 days' },
  { value: 'Last week' },
  { value: 'Last month' },
  { value: 'Last quarter' },
  { value: 'Last year' }
];

const GroupLabel = styled.span`
  font-size: 14px;
  margin: 0 12px;
`

const TimeGroup = ({
  members, availableMembers, addMemberName, updateMethods, title
}) => {
  const granularityMenu = (member, onClick) => (
    <Menu>
      {member.granularities.length ? member.granularities.map(m => (
        <Menu.Item key={m.title} onClick={() => onClick(m)}>
          {m.title}
        </Menu.Item>
      )) : <Menu.Item disabled>No members found</Menu.Item>}
    </Menu>
  );

  const dateRangeMenu = (onClick) => (
    <Menu>
      {DateRanges.map(m => (
        <Menu.Item key={m.title || m.value} onClick={() => onClick(m)}>
          {m.title || m.value}
        </Menu.Item>
      ))}
    </Menu>
  );

  return (
    <div>
      <MemberGroupTitle title={title} />
      {members.map(m => [
        <RemoveButtonGroup onRemoveClick={() => updateMethods.remove(m)} key={`${m.dimension.name}-member`}>
          <MemberDropdown
            type="selected"
            onClick={updateWith => updateMethods.update(m, { ...m, dimension: updateWith })}
            availableMembers={availableMembers}
          >
            {m.dimension.title}
          </MemberDropdown>
        </RemoveButtonGroup>,
        <GroupLabel key={`${m.dimension.name}-for`}>for</GroupLabel>,
        <ButtonDropdown
          type="time-group"
          overlay={dateRangeMenu(dateRange => updateMethods.update(m, { ...m, dateRange: dateRange.value }))}
          key={`${m.dimension.name}-date-range`}
        >
          {m.dateRange || 'All time'}
        </ButtonDropdown>,
        <GroupLabel key={`${m.dimension.name}-by`}>by</GroupLabel>,
        <ButtonDropdown
          type="time-group"
          overlay={granularityMenu(
            m.dimension,
            granularity => updateMethods.update(m, { ...m, granularity: granularity.name })
          )}
          key={`${m.dimension.name}-granularity`}
        >
          {
            m.dimension.granularities.find(g => g.name === m.granularity)
            && m.dimension.granularities.find(g => g.name === m.granularity).title
          }
        </ButtonDropdown>
      ])}
      {!members.length && (
        <MemberDropdown
          onClick={member => updateMethods.add({ dimension: member, granularity: 'day' })}
          availableMembers={availableMembers}
          type="new"
        >
          {addMemberName}
           <PlusIcon />
        </MemberDropdown>
      )}
    </div>
  );
};

export default TimeGroup;
```
Finally, update the `<FilterGroup />` component in `src/components/QueryBuilder/FilterGroup.js`.

```jsx
import React from 'react';
import { Select } from 'antd';
import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import FilterInput from './FilterInput';
import PlusIcon from './PlusIcon';

const FilterGroup = ({
  members, availableMembers, addMemberName, updateMethods
}) => (
  <span>
    {members.map(m => (
      <div style={{ marginBottom: 12 }} key={m.index}>
        <RemoveButtonGroup onRemoveClick={() => updateMethods.remove(m)}>
          <MemberDropdown
            type="selected-filter"
            onClick={updateWith => updateMethods.update(m, { ...m, dimension: updateWith })}
            availableMembers={availableMembers}
            style={{
              width: 150,
              textOverflow: 'ellipsis',
              overflow: 'hidden'
            }}
          >
            {m.dimension.title}
          </MemberDropdown>
        </RemoveButtonGroup>
        <Select
          value={m.operator}
          onChange={(operator) => updateMethods.update(m, { ...m, operator })}
          style={{ width: 200, marginRight: 8 }}
        >
          {m.operators.map(operator => (
            <Select.Option
              key={operator.name}
              value={operator.name}
            >
              {operator.title}
            </Select.Option>
          ))}
        </Select>
        <FilterInput member={m} key="filterInput" updateMethods={updateMethods}/>
      </div>
    ))}
    <MemberDropdown
      onClick={(m) => updateMethods.add({ dimension: m })}
      availableMembers={availableMembers}
      type="new"
    >
      {addMemberName}
      <PlusIcon />
    </MemberDropdown>
  </span>
);

export default FilterGroup;
```

The last component we're going to update is `<SelectChartType />`. Replace the
content of `src/components/QueryBuilder/SelectChartType.js` with the
following.

```jsx
import React from 'react';
import {
  Menu, Icon, Dropdown
} from 'antd';

import styled from 'styled-components';

const StyledDropdownTrigger = styled.span`
  color: #43436B;
  cursor: pointer;
  margin-left: 13px;

  & > span {
    margin: 0 8px;
  }
`

const ChartTypes = [
  { name: 'line', title: 'Line', icon: 'line-chart' },
  { name: 'area', title: 'Area', icon: 'area-chart' },
  { name: 'bar', title: 'Bar', icon: 'bar-chart' },
  { name: 'pie', title: 'Pie', icon: 'pie-chart' },
  { name: 'table', title: 'Table', icon: 'table' },
  { name: 'number', title: 'Number', icon: 'info-circle' }
];

const SelectChartType = ({ chartType, updateChartType }) => {
  const menu = (
    <Menu>
      {ChartTypes.map(m => (
        <Menu.Item key={m.title} onClick={() => updateChartType(m.name)}>
          <Icon type={m.icon} />
          &nbsp;{m.title}
        </Menu.Item>
      ))}
    </Menu>
  );

  const foundChartType = ChartTypes.find(t => t.name === chartType);
  return (
    <Dropdown overlay={menu} icon={foundChartType.icon} lacement="bottomLeft" trigger={['click']}>
    <StyledDropdownTrigger>
      <Icon type={foundChartType.icon} />
      <span>{foundChartType.title}</span>
      <Icon type="caret-down" />
    </StyledDropdownTrigger>
    </Dropdown>
  );
};

export default SelectChartType;
```

That's it! Those were a lot of changes, but now we
have a fully custom query builder. I hope it gives you an idea of how you can
customize such a component to fit your design.

Next, we are going to style the charts.
