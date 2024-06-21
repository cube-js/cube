import { PlusOutlined } from '@ant-design/icons';
import { Menu } from 'antd';
import { Fragment, useState } from 'react';
import styled from 'styled-components';

import { SectionRow } from '../components';
import { ButtonDropdown } from './ButtonDropdown';
import MemberDropdown from './MemberDropdown';
import MissingMemberTooltip from './MissingMemberTooltip';
import RemoveButtonGroup from './RemoveButtonGroup';
import { TimeDateRangeSelector } from './TimeRangeSelector';

const Label = styled.div`
  color: var(--dark-04-color);
  line-height: 32px;
`;

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
  const [shown, setShown] = useState(false);
  const [granularityShown, setGranularityShown] = useState(false);
  const isCustomDateRange = Array.isArray(members[0]?.dateRange);
  const [isRangePickerVisible, toggleRangePicker] = useState(false);

  function onDateRangeSelect(m, dateRange) {
    if (dateRange && !dateRange.some((d) => !d)) {
      updateMethods.update(m, {
        ...m,
        dateRange,
      });
    }
  }

  const granularityMenu = (member, onClick) => (
    <div className="test simple-overlay">
      <Menu className="ant-dropdown-menu ant-dropdown-menu-root">
        {member.granularities.length ? (
          member.granularities.map((m) => (
            <Menu.Item
              key={m.title}
              className="ant-dropdown-menu-item"
              onClick={() => onClick(m)}
            >
              {m.title}
            </Menu.Item>
          ))
        ) : (
          <Menu.Item disabled>No members found</Menu.Item>
        )}
      </Menu>
    </div>
  );

  const dateRangeMenu = (onClick) => (
    <div className="test simple-overlay">
      <Menu className="ant-dropdown-menu ant-dropdown-menu-root">
        {DateRanges.map((m) => (
          <Menu.Item
            key={m.title || m.value}
            className="ant-dropdown-menu-item"
            onClick={() => onClick(m)}
          >
            {m.title || m.value}
          </Menu.Item>
        ))}
      </Menu>
    </div>
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
              data-testid="TimeDimension"
              disabled={disabled}
              availableCubes={availableMembers}
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
              <MissingMemberTooltip>{buttonGroup}</MissingMemberTooltip>
            ) : (
              buttonGroup
            )}
            <Label>for</Label>

            <ButtonDropdown
              show={shown}
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
              onOverlayOpen={() => setShown(true)}
              onOverlayClose={() => setShown(false)}
              onItemClick={() => setShown(false)}
            >
              {isRangePickerVisible || isCustomDateRange
                ? 'Custom'
                : m.dateRange || 'All time'}
            </ButtonDropdown>

            {isRangePickerVisible || isCustomDateRange ? (
              <TimeDateRangeSelector
                value={parsedDateRange || []}
                onChange={(dateRange) => {
                  onDateRangeSelect(m, dateRange);
                }}
              />
            ) : null}

            <Label>by</Label>

            <ButtonDropdown
              show={granularityShown}
              disabled={disabled}
              overlay={granularityMenu(m.dimension, (granularity) =>
                updateMethods.update(m, { ...m, granularity: granularity.name })
              )}
              onOverlayOpen={() => setGranularityShown(true)}
              onOverlayClose={() => setGranularityShown(false)}
              onItemClick={() => setGranularityShown(false)}
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
          data-testid="TimeDimension"
          disabled={disabled}
          availableCubes={availableMembers}
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
