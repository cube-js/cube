import { InfoCircleOutlined, ArrowRightOutlined } from '@ant-design/icons';
import { Modal, Typography } from 'antd';

import { Flex, Box } from '../../../grid';
import { useRollupDesignerContext } from '../Context';
import { RollupDesigner } from '../RollupDesigner';

const { Link } = Typography;

type RollupDesignerModalProps = {
  apiUrl: string;
  token?: string;
  onAfterClose: () => void;
};

export function RollupDesignerModal({
  onAfterClose,
  ...props
}: RollupDesignerModalProps) {
  const { isModalOpen, toggleModal, memberTypeCubeMap } =
    useRollupDesignerContext();

  return (
    <Modal
      title="Rollup Designer"
      visible={isModalOpen}
      bodyStyle={{ padding: 0 }}
      destroyOnClose
      wrapClassName="rollup-designer"
      footer={
        <Link
          href="https://cube.dev/docs/caching/pre-aggregations/getting-started"
          target="_blank"
        >
          <Flex justifyContent="center" gap={1}>
            <Box>
              <InfoCircleOutlined />
            </Box>

            <Box>Further reading about pre-aggregations for reference.</Box>

            <Box>
              <ArrowRightOutlined />
            </Box>
          </Flex>
        </Link>
      }
      width={1190}
      afterClose={onAfterClose}
      onCancel={() => toggleModal()}
    >
      <div data-testid="rd-modal">
        <RollupDesigner
          apiUrl={props.apiUrl}
          token={props.token}
          memberTypeCubeMap={memberTypeCubeMap}
        />
      </div>
    </Modal>
  );
}
