import { Divider, InputNumber, Spin } from 'antd';
import Text from 'antd/lib/typography/Text';
import { useState } from 'react';

import Axes from '../Pivot/Axes';
import Options from '../Pivot/Options';
import OrderGroup from '../Order/OrderGroup';
import { ButtonDropdown } from '../../QueryBuilder/ButtonDropdown';
import styled from 'styled-components';

const PivotPopover = styled.div`
  background: #fff;
  width: 450px;
`;

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
  const [pivotShown, setPivotShown] = useState(false);
  const [orderShown, setOrderShown] = useState(false);
  const [limitShown, setLimitShown] = useState(false);

  const [limit, setLimit] = useState<number>(initialLimit);

  return (
    <>
      <Text style={{ lineHeight: '32px' }}>Settings:</Text>

      <ButtonDropdown
        show={pivotShown}
        disabled={!isQueryPresent || disabled}
        style={{ border: 0 }}
        overlay={
          pivotConfig === null ? (
            <Spin />
          ) : (
            <PivotPopover data-testid="pivot-popover">
              <Axes pivotConfig={pivotConfig} onMove={onMove} />
              <Divider style={{ margin: 0 }} />
              <div style={{ padding: '8px' }}>
                <Options pivotConfig={pivotConfig} onUpdate={onUpdate} />
              </div>
            </PivotPopover>
          )
        }
        onOverlayOpen={() => setPivotShown(true)}
        onOverlayClose={() => setPivotShown(false)}
      >
        Pivot
      </ButtonDropdown>

      <ButtonDropdown
        data-testid="order-btn"
        show={orderShown}
        disabled={!isQueryPresent || disabled}
        style={{ border: 0 }}
        overlay={
          <div
            style={{
              padding: '8px',
              paddingBottom: 1,
              width: 400,
              backgroundColor: '#fff',
            }}
          >
            <OrderGroup
              orderMembers={orderMembers}
              onReorder={onReorder}
              onOrderChange={onOrderChange}
            />
          </div>
        }
        onOverlayOpen={() => setOrderShown(true)}
        onOverlayClose={() => setOrderShown(false)}
      >
        Order
      </ButtonDropdown>

      <ButtonDropdown
        data-testid="limit-btn"
        show={limitShown}
        disabled={!isQueryPresent || disabled}
        style={{ border: 0 }}
        overlay={
          <div
            style={{
              padding: '8px',
              background: 'white',
            }}
          >
            <label>
              <InputNumber
                prefix="Limit"
                type="number"
                value={limit}
                step={500}
                min={0}
                onChange={setLimit}
                onPressEnter={() => {
                  onUpdate({ limit });
                  setLimitShown(false);
                }}
              />
            </label>
          </div>
        }
        onOverlayOpen={() => setLimitShown(true)}
        onOverlayClose={() => setLimitShown(false)}
      >
        Limit
      </ButtonDropdown>

    </>
  );
}
