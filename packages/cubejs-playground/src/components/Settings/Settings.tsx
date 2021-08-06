import { Divider, InputNumber, Spin } from 'antd';
import Text from 'antd/lib/typography/Text';
import { useState } from 'react';

import Axes from '../Pivot/Axes';
import Options from '../Pivot/Options';
import OrderGroup from '../Order/OrderGroup';
import { Button, Popover } from '../../atoms';

export default function Settings({
  pivotConfig,
  orderMembers,
  limit: initialLimit,
  disabled,
  onMove,
  onUpdate,
  onReorder,
  onOrderChange,
  isQueryPresent,
}) {
  const [limit, setLimit] = useState<number>(initialLimit);
  const [isLimitPopoverVisible, setIsLimitPopoverVisible] =
    useState<boolean>(false);

  return (
    <>
      <Text style={{ lineHeight: '32px' }}>Settings:</Text>
      <Popover
        content={
          pivotConfig === null ? (
            <Spin />
          ) : (
            <div data-testid="pivot-popover">
              <Axes pivotConfig={pivotConfig} onMove={onMove} />
              <Divider style={{ margin: 0 }} />
              <div style={{ padding: '8px' }}>
                <Options pivotConfig={pivotConfig} onUpdate={onUpdate} />
              </div>
            </div>
          )
        }
        placement="bottomLeft"
        trigger="click"
      >
        <Button
          data-testid="pivot-btn"
          disabled={!isQueryPresent || disabled}
          style={{ border: 0 }}
        >
          Pivot
        </Button>
      </Popover>

      <Popover
        content={
          <div
            style={{
              padding: '8px',
              paddingBottom: 1,
            }}
          >
            <OrderGroup
              orderMembers={orderMembers}
              onReorder={onReorder}
              onOrderChange={onOrderChange}
            />
          </div>
        }
        placement="bottomLeft"
        trigger="click"
      >
        <Button
          data-testid="order-btn"
          disabled={!isQueryPresent || disabled}
          style={{ border: 0 }}
        >
          Order
        </Button>
      </Popover>

      <Popover
        visible={isLimitPopoverVisible}
        content={
          <div style={{ padding: '8px' }}>
            <label>
              Limit{' '}
              <InputNumber
                prefix="Limit"
                type="number"
                value={limit}
                step={500}
                onChange={setLimit}
                onPressEnter={() => {
                  onUpdate({ limit });
                  setIsLimitPopoverVisible(false);
                }}
              />
            </label>
          </div>
        }
        placement="bottomLeft"
        trigger="click"
        onVisibleChange={(visible) => {
          setIsLimitPopoverVisible(visible);

          if (!visible) {
            onUpdate({ limit });
          }
        }}
      >
        <Button
          data-testid="limit-btn"
          disabled={!isQueryPresent || disabled}
          style={{ border: 0 }}
        >
          Limit
        </Button>
      </Popover>
    </>
  );
}
