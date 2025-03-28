import { ReactNode } from 'react';

import { getTypeIcon } from '../utils';
import { MemberViewType } from '../types';
import { useShownMemberName } from '../hooks';

import { MemberLabelText } from './MemberLabelText';
import { MemberBadge } from './Badge';

function StyledTypeIcon(props: {
  isMissing?: boolean;
  memberType: 'measure' | 'dimension' | 'timeDimension' | 'segment';
  type?: 'number' | 'string' | 'time' | 'boolean' | 'filter';
}) {
  const { type, memberType, isMissing } = props;
  const memberColorName = isMissing ? 'danger' : memberType;

  return (
    <span
      style={{
        display: 'grid',
        color: `var(--${memberColorName}-text-color)`,
        placeSelf: 'center',
      }}
    >
      {getTypeIcon(type)}
    </span>
  );
}

interface MemberLabelProps {
  name: string;
  memberName?: string;
  cubeName?: string;
  memberTitle?: string;
  cubeTitle?: string;
  memberViewType?: MemberViewType;
  isCompact?: boolean;
  memberType?: 'measure' | 'dimension' | 'timeDimension' | 'segment';
  type?: 'number' | 'string' | 'time' | 'boolean' | 'filter';
  isMissing?: boolean;
  children?: ReactNode;
}

export function MemberLabel(props: MemberLabelProps) {
  const {
    name,
    cubeName = props.name.split('.')[0],
    cubeTitle,
    memberName = props.name.split('.')[1],
    memberTitle,
    isCompact,
    memberType,
    type,
    memberViewType,
    isMissing,
    children,
  } = props;
  const arr = name.split('.');

  const { shownMemberName, shownCubeName } = useShownMemberName({
    cubeName,
    cubeTitle,
    memberName,
    memberTitle,
    type: memberViewType,
  });

  return (
    <MemberLabelText data-member={memberType} mods={{ missing: isMissing }}>
      {memberType ? (
        <StyledTypeIcon isMissing={isMissing} type={type} memberType={memberType} />
      ) : null}
      {!isCompact || !cubeName ? (
        <>
          <span data-element="Name">
            <span data-element="CubeName">{shownCubeName}</span>
            <span data-element="Divider">{memberViewType === 'name' ? '.' : <>&nbsp;</>}</span>
            <span data-element="MemberName">{shownMemberName}</span>
          </span>
          {arr[2] ? (
            <span data-element="Grouping">
              <MemberBadge isSpecial type={memberType}>
                {arr[2]}
              </MemberBadge>
            </span>
          ) : null}
        </>
      ) : (
        <span data-element="MemberName">{shownMemberName}</span>
      )}
      {children}
    </MemberLabelText>
  );
}
