import { PlusOutlined } from '@ant-design/icons';
import { DatePicker, Menu } from 'antd';
import moment from 'moment';
import { useState, Fragment } from 'react';
import styled from 'styled-components';

import ButtonDropdown from './ButtonDropdown';
import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import MissingMemberTooltip from './MissingMemberTooltip';
import { SectionRow } from '../components';

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
  disabled = false,
  availableMembers,
  missingMembers,
  addMemberName,
  updateMethods,
  parsedDateRange,
}: any) => {
  const isCustomDateRange = Array.isArray(members[0]?.dateRange);
  const [isRangePickerVisible, toggleRangePicker] = useState(false);

  function onDateRangeSelect(m, dateRange) {
    if (dateRange && !dateRange.some((d) => !d)) {
      updateMethods.update(m, {
        ...m,
        dateRange: dateRange.map((dateTime) => dateTime.format('YYYY-MM-DD')),
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
      {members.map((m, index) => {
        const isMissing = missingMembers.includes(m.dimension.title);

        const buttonGroup = (
          <RemoveButtonGroup
            disabled={disabled}
            className={disabled ? 'disabled' : null}
            color={isMissing ? 'danger' : 'primary'}
            onRemoveClick={() => updateMethods.remove(m)}
          >
            <MemberDropdown
              disabled={disabled}
              availableMembers={availableMembers}
              onClick={(updateWith) =>
                updateMethods.update(m, { ...m, dimension: updateWith })
              }
            >
              {m.dimension.title}
            </MemberDropdown>
          </RemoveButtonGroup>
        );

        return (
          <Fragment key={index}>
            {isMissing ? (
              <MissingMemberTooltip>
                {buttonGroup}
              </MissingMemberTooltip>
            ) : (
              buttonGroup
            )}
            <Label>for</Label>

            <ButtonDropdown
              disabled={disabled}
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
              {isRangePickerVisible || isCustomDateRange
                ? 'Custom'
                : m.dateRange || 'All time'}
            </ButtonDropdown>

            {isRangePickerVisible || isCustomDateRange ? (
              <RangePicker
                disabled={disabled}
                format="YYYY-MM-DD"
                defaultValue={(parsedDateRange || []).map((date) =>
                  moment(date)
                )}
                onChange={(dateRange) => onDateRangeSelect(m, dateRange)}
              />
            ) : null}

            <Label>by</Label>

            <ButtonDropdown
              disabled={disabled}
              overlay={granularityMenu(m.dimension, (granularity) =>
                updateMethods.update(m, { ...m, granularity: granularity.name })
              )}
            >
              {m.dimension.granularities.find(
                (g) => g.name === m.granularity
              ) &&
                m.dimension.granularities.find((g) => g.name === m.granularity)
                  .title}
            </ButtonDropdown>
          </Fragment>
        );
      })}

      {!members.length && (
        <MemberDropdown
          disabled={disabled}
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
