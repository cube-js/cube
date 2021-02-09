import { InputNumber } from 'antd';

export default function Limit({ limit = 5000, onUpdate }) {
  return (
    <label>
      Limit{' '}
      <InputNumber
        prefix="Limit"
        value={limit}
        step={500}
        onChange={(value) =>
          onUpdate({
            limit: value,
          })
        }
      />
    </label>
  );
}
