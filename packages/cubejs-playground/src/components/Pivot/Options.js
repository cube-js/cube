import React from 'react';
import { Row, Col, Checkbox, InputNumber } from 'antd';

export default function Options({ pivotConfig, limit = 10000, onUpdate }) {
  return (
    <Row gutter={[0, 12]}>
      <Col span={24}>
        <Checkbox
          checked={pivotConfig.fillMissingDates}
          onChange={() =>
            onUpdate({
              fillMissingDates: !pivotConfig.fillMissingDates,
            })
          }
        >
          Fill Missing Dates
        </Checkbox>
      </Col>

      <Col span={24}>
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
      </Col>
    </Row>
  );
}
