import { CopyOutlined } from '@ant-design/icons';
import { Button, Form, Input } from 'antd';
import { InputProps } from 'antd/lib/input/Input';
import styled from 'styled-components';

const IconButton: typeof Button = styled(Button)`
  border: none;
  color: var(--primary-1);
`;

type CopiableInputProps = {
  label?: string;
  onCopyClick: (value: string) => void;
} & InputProps;

export function CopiableInput({
  label,
  value,
  onCopyClick,
  ...props
}: CopiableInputProps) {
  const suffix = (
    <IconButton
      data-testid={`localhost-tipbox-${label?.toLowerCase()}-copy-btn`}
      icon={<CopyOutlined />}
      onClick={() => value && onCopyClick(value.toString())}
    />
  );

  return (
    <Form.Item
      label={label ? <b>{label}</b> : null}
      labelCol={{ span: 24 }}
      wrapperCol={{ span: 24 }}
    >
      <Input
        data-testid={`localhost-tipbox-${label?.toLowerCase()}-input`}
        value={value}
        suffix={suffix}
        {...props}
      />
    </Form.Item>
  );
}
