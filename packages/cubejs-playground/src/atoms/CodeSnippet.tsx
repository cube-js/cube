import styled from 'styled-components';
import { CSSProperties } from "react";
import { Button } from 'antd';
import { CopyOutlined } from '@ant-design/icons';

import PrismCode from '../PrismCode';
import { copyToClipboard } from '../utils';

type CodeSnippetProps = {
  code: string;
  language?: string;
  style?: CSSProperties;
};

const StyledCodeSnippet = styled.div`
  display: flex;
  border-radius: 4px;
  background: var(--layout-body-background);
  width: 100%;
  max-width: 100%;
`;

const ButtonWrapper = styled.div`
  position: relative;

  button,
  button:hover,
  button:focus {
    border: none;
    background: none;
    box-shadow: none;
    outline: none;
    color: var(--primary-color);
  }

  [ant-click-animating-without-extra-node]:after {
    animation: none !important;
  }

  ::after {
    display: block;
    content: '';
    width: 16px;
    position: absolute;
    left: -16px;
    top: 0;
    bottom: 0;
    background: linear-gradient(
      to right,
      rgba(246, 246, 248, 0),
      rgba(246, 246, 248, 1)
    );
  }
`;

export function CodeSnippet({ code, language, style }: CodeSnippetProps) {
  return (
    <StyledCodeSnippet style={style}>
      <PrismCode code={code} language={language} style={{ flexGrow: 1 }} />

      <ButtonWrapper>
        <Button
          icon={<CopyOutlined />}
          onClick={() =>
            copyToClipboard(code, 'Pre-aggregation code is copied')
          }
        />
      </ButtonWrapper>
    </StyledCodeSnippet>
  );
}
