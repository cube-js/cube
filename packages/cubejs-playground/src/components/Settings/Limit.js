import { InputNumber } from 'antd';

export default function Limit({ limit = 200, onUpdate }) {
  return (
    <label>
      Limit{' '}
      <InputNumber
        prefix="Limit"
        value={limit}
        step={100}
        onChange={(value) =>
          onUpdate({
            limit: value,
          })
        }
      />
    </label>
  );
}
