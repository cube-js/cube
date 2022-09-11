import { Tag, TagProps } from 'antd';
import styled from 'styled-components';

const StyledTag = styled(Tag)`
  background-color: var(--primary-9);
  color: var(--primary-color);
  padding: 6px 10px;
  font-size: var(--font-size-base);
  border: none;

  .ant-tag-close-icon {
    color: var(--primary-color);
    padding-left: 6px;
  }

  b {
    font-weight: 600;
  }
`;

type MemberTagProps = {
  name: string;
  cubeName: string;
};

export function MemberTag({
  name,
  cubeName,
  ...props
}: MemberTagProps & TagProps) {
  return (
    <StyledTag
      data-testid={`member-tag-${cubeName}.${name}`}
      closable
      visible
      {...props}
    >
      {cubeName} <b>{name}</b>
    </StyledTag>
  );
}
