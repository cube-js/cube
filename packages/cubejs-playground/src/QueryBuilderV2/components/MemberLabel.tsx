import { getTypeIcon } from '../utils';

import { MemberLabelText } from './MemberLabelText';
import { MemberBadge } from './Badge';

function StyledTypeIcon(props: {
  member: 'measure' | 'dimension' | 'timeDimension' | 'missing';
  type: 'number' | 'string' | 'time' | 'boolean' | 'filter';
}) {
  const { type, member } = props;
  const memberColorName = member === 'missing' ? 'danger' : member;

  return (
    <span
      style={{
        display: 'grid',
        color: `var(--${memberColorName}-text-color)`,
        placeSelf: 'center',
      }}
    >
      {getTypeIcon(type || 'number')}
    </span>
  );
}

interface MemberLabelProps {
  name: string;
  member?: 'measure' | 'dimension' | 'timeDimension';
  type?: 'number' | 'string' | 'time' | 'boolean' | 'filter';
}

export function MemberLabel(props: MemberLabelProps) {
  const { name, member, type } = props;

  const arr = name.split('.');

  return (
    <MemberLabelText data-member={member}>
      {type && member ? <StyledTypeIcon type={type} member={member} /> : null}
      {arr.length > 1 ? (
        <>
          <span data-element="Name">
            <span data-element="CubeName">{arr[0]}</span>
            <span data-element="Divider">.</span>
            <span data-element="MemberName">{arr[1]}</span>
          </span>
          {arr[2] ? (
            <span data-element="Grouping">
              <MemberBadge isSpecial type={member}>
                {arr[2]}
              </MemberBadge>
            </span>
          ) : null}
        </>
      ) : (
        <span data-element="MemberName">{name}</span>
      )}
    </MemberLabelText>
  );
}
