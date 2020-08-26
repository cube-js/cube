import React, { useState } from 'react';
import * as PropTypes from 'prop-types';
import { Menu, DatePicker } from 'antd';
import { PlusOutlined } from '@ant-design/icons';

import ButtonDropdown from './ButtonDropdown';
import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';

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
  members,
  availableMembers,
  addMemberName,
  updateMethods,
}) => {
  const [customDateRange, setCustomDateRange] = useState(false);

  function onDateRangeSelect(m, dateRange) {
    if (!dateRange.some((d) => !d)) {
      updateMethods.update(m, {
        ...m,
        dateRange: dateRange.map((dateTime) =>
          dateTime.format('YYYY-MM-DDTHH:mm:00.000')
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
    <>
      {members.map((m) => (
        <>
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

          <b>FOR</b>

          <ButtonDropdown
            overlay={dateRangeMenu((dateRange) => {
              if (dateRange.value === 'custom') {
                setCustomDateRange(true);
              } else {
                updateMethods.update(m, {
                  ...m,
                  dateRange: dateRange.value,
                });
                setCustomDateRange(false);
              }
            })}
            style={{
              marginLeft: 8,
              marginRight: 8,
            }}
          >
            {(customDateRange && 'Custom') || m.dateRange || 'All time'}
          </ButtonDropdown>

          {customDateRange ? (
            <RangePicker
              style={{ marginRight: 8 }}
              format="YYYY-MM-DD"
              onOk={(dateRange) => onDateRangeSelect(m, dateRange)}
            />
          ) : null}

          <b>BY</b>

          <ButtonDropdown
            overlay={granularityMenu(m.dimension, (granularity) =>
              updateMethods.update(m, { ...m, granularity: granularity.name })
            )}
            style={{ marginLeft: 8 }}
          >
            {m.dimension.granularities.find((g) => g.name === m.granularity) &&
              m.dimension.granularities.find((g) => g.name === m.granularity)
                .title}
          </ButtonDropdown>
        </>
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
    </>
  );
};

TimeGroup.propTypes = {
  members: PropTypes.array.isRequired,
  availableMembers: PropTypes.array.isRequired,
  addMemberName: PropTypes.string.isRequired,
  updateMethods: PropTypes.object.isRequired,
};

export default TimeGroup;
