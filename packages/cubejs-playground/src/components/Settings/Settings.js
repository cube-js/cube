import { Divider, Spin } from 'antd';
import Axes from '../Pivot/Axes';
import Options from '../Pivot/Options';
import OrderGroup from '../Order/OrderGroup';
import Limit from './Limit';
import { Button, Popover } from '../../components';
import Text from 'antd/lib/typography/Text';

export default function Settings({
  pivotConfig,
  orderMembers,
  limit,
  onMove,
  onUpdate,
  onReorder,
  onOrderChange,
  isQueryPresent,
}) {
  return (
    <>
      <Text style={{ lineHeight: '32px' }}>Settings:</Text>
      <Popover
        content={
          pivotConfig === null ? (
            <Spin />
          ) : (
            <>
              <Axes pivotConfig={pivotConfig} onMove={onMove} />
              <Divider style={{ margin: 0 }} />
              <div style={{ padding: '8px' }}>
                <Options pivotConfig={pivotConfig} onUpdate={onUpdate} />
              </div>
            </>
          )
        }
        placement="bottomLeft"
        trigger="click"
      >
        <Button disabled={!isQueryPresent} style={{ border: 0 }}>
          Pivot
        </Button>
      </Popover>

      <Popover
        content={
          <div style={{ padding: '8px' }}>
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
        <Button disabled={!isQueryPresent} style={{ border: 0 }}>
          Order
        </Button>
      </Popover>

      <Popover
        content={
          <div style={{ padding: '8px' }}>
            <Limit limit={limit} onUpdate={onUpdate} />
          </div>
        }
        placement="bottomLeft"
        trigger="click"
      >
        <Button disabled={!isQueryPresent} style={{ border: 0 }}>
          Limit
        </Button>
      </Popover>
    </>
  );
}
