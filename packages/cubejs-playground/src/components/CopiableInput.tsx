import { CopyOutlined } from '@ant-design/icons';
import { Button, Form, Input } from 'antd';
import { InputProps } from 'antd/lib/input/Input';
import styled from 'styled-components';
import { copyToClipboard } from '../utils';

const IconButton: typeof Button = styled(Button)`
  border: none;
  color: var(--primary-1);
`;

type CopiableInputProps = {
  label?: string;
  wrapperStyle?: object;
  onCopyClick?: (value: string) => void;
} & InputProps;

export function CopiableInput({
  label,
  value,
  onCopyClick,
  wrapperStyle,
  ...props
}: CopiableInputProps) {
  const suffix = (
    <IconButton
      data-testid={`localhost-tipbox-${label?.toLowerCase()}-copy-btn`}
      icon={<CopyOutlined />}
      onClick={async () => {
        if (value) {
          if (onCopyClick != null) {
            onCopyClick(value.toString());
          } else {
            await copyToClipboard(value.toString());
          }
        }
      }}
    />
  );

  return (
    <Form.Item
      label={label ? <b>{label}</b> : null}
      labelCol={{ span: 24 }}
      wrapperCol={{ span: 24 }}
      style={wrapperStyle}
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
