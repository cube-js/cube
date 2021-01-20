import { PlusOutlined } from '@ant-design/icons';
import { DatePicker, Menu } from 'antd';
import moment from 'moment';
import { useState, Fragment } from 'react';
import ButtonDropdown from './ButtonDropdown';
import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import { SectionRow } from '../components';
import styled from 'styled-components';

const Label = styled.div`
  color: var(--dark-04-color);
  line-height: 32px;
`;

const { RangePicker } = DatePicker;

const DateRanges = [
  { title: 'Custom', value: 'custom' },
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
  { value: 'Last year' },
];

const TimeGroup = ({
  members = [],
  availableMembers,
  addMemberName,
  updateMethods,
  parsedDateRange,
}) => {
  const isCustomDateRange = Array.isArray(members[0]?.dateRange);
  const [isRangePickerVisible, toggleRangePicker] = useState(false);

  function onDateRangeSelect(m, dateRange) {
    if (dateRange && !dateRange.some((d) => !d)) {
      updateMethods.update(m, {
        ...m,
        dateRange: dateRange.map((dateTime) =>
          dateTime.format('YYYY-MM-DD')
        ),
      });
    }
  }

  const granularityMenu = (member, onClick) => (
    <Menu>
      {member.granularities.length ? (
        member.granularities.map((m) => (
          <Menu.Item key={m.title} onClick={() => onClick(m)}>
            {m.title}
          </Menu.Item>
        ))
      ) : (
        <Menu.Item disabled>No members found</Menu.Item>
      )}
    </Menu>
  );

  const dateRangeMenu = (onClick) => (
    <Menu>
      {DateRanges.map((m) => (
        <Menu.Item key={m.title || m.value} onClick={() => onClick(m)}>
          {m.title || m.value}
        </Menu.Item>
      ))}
    </Menu>
  );

  return (
    <SectionRow>
      {members.map((m, index) => (
        <Fragment key={index}>
          <RemoveButtonGroup onRemoveClick={() => updateMethods.remove(m)}>
            <MemberDropdown
              onClick={(updateWith) =>
                updateMethods.update(m, { ...m, dimension: updateWith })
              }
              availableMembers={availableMembers}
            >
              {m.dimension.title}
            </MemberDropdown>
          </RemoveButtonGroup>

          <Label>for</Label>

          <ButtonDropdown
            overlay={dateRangeMenu((dateRange) => {
              if (dateRange.value === 'custom') {
                toggleRangePicker(true);
              } else {
                updateMethods.update(m, {
                  ...m,
                  dateRange: dateRange.value,
                });
                toggleRangePicker(false);
              }
            })}
          >
            {(isRangePickerVisible || isCustomDateRange) ? 'Custom' : m.dateRange || 'All time'}
          </ButtonDropdown>

          {isRangePickerVisible || isCustomDateRange ? (
            <RangePicker
              format="YYYY-MM-DD"
              defaultValue={(parsedDateRange || []).map((date) => moment(date))}
              onChange={(dateRange) => onDateRangeSelect(m, dateRange)}
            />
          ) : null}

          <Label>by</Label>

          <ButtonDropdown
            overlay={granularityMenu(m.dimension, (granularity) =>
              updateMethods.update(m, { ...m, granularity: granularity.name })
            )}
          >
            {m.dimension.granularities.find((g) => g.name === m.granularity) &&
              m.dimension.granularities.find((g) => g.name === m.granularity)
                .title}
          </ButtonDropdown>
        </Fragment>
      ))}

      {!members.length && (
        <MemberDropdown
          availableMembers={availableMembers}
          type="dashed"
          icon={<PlusOutlined />}
          onClick={(member) =>
            updateMethods.add({ dimension: member, granularity: 'day' })
          }
        >
          {addMemberName}
        </MemberDropdown>
      )}
    </SectionRow>
  );
};

export default TimeGroup;
