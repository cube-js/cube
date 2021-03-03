import styled from 'styled-components';
import { Select as AntdSelect } from 'antd';

const StyledSelect = styled(AntdSelect)`
  &&& {
    &.ant-select-disabled {
      .ant-select-selector {
        border-color: var(--disabled-color);
        background-color: white;
      }

      .ant-select-selector {
        color: var(--disabled-color);
        
        &:hover,
        &:active,
        &:focus {
          border-color: var(--disabled-color);
          color: var(--disabled-color);
        }
      }
    }

    &.ant-select-single {
      .ant-select-selector {
        padding: 5px 12px;

        .ant-select-selection-placeholder {
          line-height: 22px;
        }
      }
    }

    &.ant-select-multiple {
      .ant-select-selector {
        padding: 3px 12px;
      }

      .ant-select-selection-item {
        margin-top: 0;
        margin-bottom: 0;
      }
    }

    .ant-select-selection-item {
      font-size: 14px;
      line-height: 22px;
    }

    .ant-select-selector {
      height: auto;
      color: var(--text-color);
      font-size: 14px;
      line-height: 22px;

      &:hover,
      &:active,
      &:focus {
        border-color: var(--purple-04-color);
        color: var(--primary-color);
      }
    }

    .ant-select-selector::after {
      font-size: 14px;
      line-height: 22px;
    }
  }
`;

export default StyledSelect;
