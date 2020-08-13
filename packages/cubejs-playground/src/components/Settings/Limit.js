import React from 'react';
import { InputNumber } from 'antd';

export default function Limit({ limit = 10000, onUpdate }) {
  return (
    <label>
      Limit{' '}
      <InputNumber
        prefix="Limit"
        value={limit}
        step={1000}
        onChange={(value) =>
          onUpdate({
            limit: value,
          })
        }
      />
    </label>
  );
}
